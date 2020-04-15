/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.security.kubernetes

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{ActorRef, ActorSystem, Cancellable, Props, Scheduler}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.mockito._
import org.mockito.Matchers.{any, same}
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.scalatest.{BeforeAndAfter, FunSuiteLike}

import org.apache.spark.security.kubernetes.constants._


class TokenRefreshServiceSuite extends TestKit(ActorSystem("test"))
  with FunSuiteLike with BeforeAndAfter {

  private val configKeyPrefix = "hadoop-token-refresh-server"
  private val configMap = Map(s"$configKeyPrefix.kerberosPrincipal" -> "my-principal",
    s"$configKeyPrefix.scanAllNamespaces" -> true,
    s"$configKeyPrefix.namespaceToScan" -> "my-namespace")
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var kubernetesClient: KubernetesClient = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var scheduler: Scheduler = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var ugi: UgiUtil = _
  private val config = ConfigFactory.parseMap(configMap.asJava)
  private val settings = new Settings(config)
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var clock: Clock = _
  private val nowMillis = 1500000000L
  private var actorRef: TestActorRef[TokenRefreshService] = _
  private val reloginInterval = Duration(REFRESH_SERVER_KERBEROS_RELOGIN_INTERVAL_MILLIS,
    TimeUnit.MILLISECONDS)
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var reloginCommandCancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var reloginTaskCancellable: Cancellable = _
  private val starterTaskDelay = Duration(STARTER_TASK_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS)
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var starterTaskCancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var starterTask1Cancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var starterTask2Cancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var starterTask3Cancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var renewTaskCancellable: Cancellable = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var token1: Token[_ <: TokenIdentifier] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var token2: Token[_ <: TokenIdentifier] = _

  before {
    MockitoAnnotations.initMocks(this)
    when(scheduler.schedule(Matchers.eq(reloginInterval), Matchers.eq(reloginInterval),
      any(classOf[ActorRef]),
      Matchers.eq(Relogin))(
      any(classOf[ExecutionContext]),
      any(classOf[ActorRef])))
      .thenReturn(reloginCommandCancellable)
    when(clock.nowInMillis()).thenReturn(nowMillis)
    actorRef = TestActorRef(Props(classOf[TokenRefreshService], kubernetesClient, scheduler,
      ugi, settings, clock))
  }

  test("The token refresh service actor starts properly") {
    verify(ugi).loginUserFromKeytab("my-principal", REFRESH_SERVER_KERBEROS_KEYTAB_PATH)
    verify(scheduler).schedule(Matchers.eq(reloginInterval), Matchers.eq(reloginInterval),
      same(actorRef),
      Matchers.eq(Relogin))(
      any(classOf[ExecutionContext]),
      same(actorRef))
    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 1)
    assert(actor.hasExtraCancellable(Relogin.getClass, reloginCommandCancellable))
    assert(actor.numPendingSecretTasks() == 0)
    verifyNoMoreInteractions(scheduler)
  }

  test("The Relogin command launches a ReloginTask") {
    when(scheduler.scheduleOnce(any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(
      any(classOf[ExecutionContext])))
      .thenReturn(reloginTaskCancellable)
    actorRef ! Relogin

    val taskCaptor: ArgumentCaptor[Runnable] = ArgumentCaptor.forClass(classOf[Runnable])
    verify(scheduler).scheduleOnce(Matchers.eq(Duration(0, TimeUnit.MILLISECONDS)),
      taskCaptor.capture())(
      any(classOf[ExecutionContext]))
    val task = taskCaptor.getValue
    assert(task.getClass == classOf[ReloginTask])
    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 2)  // Relogin and ReloginTask
    assert(actor.hasExtraCancellable(classOf[ReloginTask], reloginTaskCancellable))
    assert(actor.numPendingSecretTasks() == 0)
  }

  test("The StartRefresh command launches a StarterTask") {
    when(scheduler.scheduleOnce(any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(
      any(classOf[ExecutionContext])))
      .thenReturn(starterTaskCancellable)
    val secret = new SecretBuilder()
        .withNewMetadata()
          .withUid("uid-0101")
        .endMetadata()
      .build()
    actorRef ! StartRefresh(secret)

    val taskCaptor: ArgumentCaptor[Runnable] = ArgumentCaptor.forClass(classOf[Runnable])
    verify(scheduler).scheduleOnce(Matchers.eq(starterTaskDelay),
      taskCaptor.capture())(
      any(classOf[ExecutionContext]))
    val task = taskCaptor.getValue
    assert(task.getClass == classOf[StarterTask])
    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 1)  // Relogin
    assert(actor.numPendingSecretTasks() == 1)
    assert(actor.hasSecretTaskCancellable("uid-0101", starterTaskCancellable))
  }

  test("The Renew command launches a RenewTask") {
    when(scheduler.scheduleOnce(any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(
      any(classOf[ExecutionContext])))
      .thenReturn(starterTaskCancellable, renewTaskCancellable)
    val secret = new SecretBuilder()
      .withNewMetadata()
      .withUid("uid-0101")
      .endMetadata()
      .build()
    actorRef ! StartRefresh(secret)
    actorRef ! Renew(expireTime = nowMillis + 10000L, Map(), secret.getMetadata,
      numConsecutiveErrors = 0)

    val taskCaptor: ArgumentCaptor[Runnable] = ArgumentCaptor.forClass(classOf[Runnable])
    val renewTaskDelay = Duration((10000L * 0.9).toLong,  // 90% of expire time from now.
      TimeUnit.MILLISECONDS)
    verify(scheduler).scheduleOnce(Matchers.eq(renewTaskDelay),
      taskCaptor.capture())(
      any(classOf[ExecutionContext]))
    val task = taskCaptor.getValue
    assert(task.getClass == classOf[RenewTask])

    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 1)  // Relogin
    assert(actor.numPendingSecretTasks() == 1)
    assert(actor.hasSecretTaskCancellable("uid-0101", renewTaskCancellable))
  }

  test("The StopRefresh command cancels a RenewTask") {
    when(scheduler.scheduleOnce(any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(
      any(classOf[ExecutionContext])))
      .thenReturn(starterTaskCancellable, renewTaskCancellable)
    val secret = new SecretBuilder()
      .withNewMetadata()
      .withUid("uid-0101")
      .endMetadata()
      .build()
    actorRef ! StartRefresh(secret)
    actorRef ! Renew(expireTime = nowMillis + 10000L, Map(), secret.getMetadata,
      numConsecutiveErrors = 0)
    actorRef ! StopRefresh(secret)

    verify(renewTaskCancellable).cancel()
    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 1)  // Relogin
    assert(actor.numPendingSecretTasks() == 0)
  }

  test("The UpdateSecretsToTrack command resets the task set to track") {
    val secret1 = new SecretBuilder()
      .withNewMetadata()
      .withUid("uid-0101")
      .endMetadata()
      .build()
    val secret2 = new SecretBuilder()
      .withNewMetadata()
      .withUid("uid-0202")
      .endMetadata()
      .build()
    val secret3 = new SecretBuilder()
      .withNewMetadata()
      .withUid("uid-0303")
      .endMetadata()
      .build()
    when(scheduler.scheduleOnce(any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(
      any(classOf[ExecutionContext])))
      .thenReturn(starterTask1Cancellable,  // for secret1
        starterTask2Cancellable,  // for secret2
        renewTaskCancellable,     // for secret2
        starterTask3Cancellable)  // for secret3
    actorRef ! UpdateSecretsToTrack(List(secret1))  // This adds a task for secret1.
    actorRef ! StartRefresh(secret2)  // Adds the secret2 task.
    actorRef ! Renew(expireTime = nowMillis + 10000L, Map(), secret2.getMetadata,
      numConsecutiveErrors = 0)
    // This removes secret1, but not the recently added secret2.
    actorRef ! UpdateSecretsToTrack(List(secret3))

    verify(starterTask1Cancellable).cancel()
    val actor: TokenRefreshService = actorRef.underlyingActor
    assert(actor.numExtraCancellables() == 1)  // Relogin
    assert(actor.numPendingSecretTasks() == 2)
    assert(actor.hasSecretTaskCancellable("uid-0202", renewTaskCancellable))
    assert(actor.hasSecretTaskCancellable("uid-0303", starterTask3Cancellable))
  }
}
