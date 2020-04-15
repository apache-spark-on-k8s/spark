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

import akka.actor.{ActorSystem, Cancellable, Scheduler}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import io.fabric8.kubernetes.api.model.{DoneableSecret, Secret, SecretList}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, FilterWatchListMultiDeletable, MixedOperation, Resource}
import org.mockito._
import org.mockito.Matchers.{any, anyString}
import org.mockito.Mockito.{doReturn, never, verify, when}
import org.scalatest.{BeforeAndAfter, FunSuiteLike}

import org.apache.spark.security.kubernetes.constants._


class SecretFinderSuite extends TestKit(ActorSystem("test")) with FunSuiteLike with BeforeAndAfter {

  private val configKeyPrefix = "hadoop-token-refresh-server"
  private val configMap1 = Map(s"$configKeyPrefix.kerberosPrincipal" -> "my-principal",
      s"$configKeyPrefix.scanAllNamespaces" -> true,
      s"$configKeyPrefix.namespaceToScan" -> "my-namespace")
  private val configMap2 = configMap1.updated(s"$configKeyPrefix.scanAllNamespaces", false)
  private val tokenRefreshServiceProbe = TestProbe()
  private val tokenRefreshService = tokenRefreshServiceProbe.ref
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var kubernetesClient: KubernetesClient = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secrets: MixedOperation[Secret, SecretList, DoneableSecret,
    Resource[Secret, DoneableSecret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretsInAnynamespace: FilterWatchListMultiDeletable[Secret, SecretList, Boolean,
    Watch, Watcher[Secret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretsInSpecifiedNamespace: FilterWatchListMultiDeletable[Secret, SecretList,
    Boolean, Watch, Watcher[Secret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretsWithLabel: FilterWatchListDeletable[Secret, SecretList, Boolean,
    Watch, Watcher[Secret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretList: SecretList = _
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private var secret1: Secret = _
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private var secret2: Secret = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var watch: Watch = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var scheduler: Scheduler = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var cancellable: Cancellable = _

  before {
    MockitoAnnotations.initMocks(this)
    when(scheduler.schedule(any(classOf[FiniteDuration]), any(classOf[FiniteDuration]),
      any(classOf[Runnable]))(any(classOf[ExecutionContext])))
      .thenReturn(cancellable)
    when(kubernetesClient.secrets).thenReturn(secrets)
    doReturn(secretsInAnynamespace).when(secrets).inAnyNamespace()
    doReturn(secretsInSpecifiedNamespace).when(secrets).inNamespace("my-namespace")
    doReturn(secretsWithLabel).when(secretsInAnynamespace).withLabel(
      SECRET_LABEL_KEY_REFRESH_HADOOP_TOKENS, SECRET_LABEL_VALUE_REFRESH_HADOOP_TOKENS)
    doReturn(secretsWithLabel).when(secretsInSpecifiedNamespace).withLabel(
      SECRET_LABEL_KEY_REFRESH_HADOOP_TOKENS, SECRET_LABEL_VALUE_REFRESH_HADOOP_TOKENS)
    when(secretsWithLabel.watch(any(classOf[Watcher[Secret]]))).thenReturn(watch)
  }

  test("Secret finder sets up the scanner and watcher, by default for all namespaces") {
    val config = ConfigFactory.parseMap(configMap1.asJava)
    val settings = new Settings(config)
    val secretFinder = SecretFinder(tokenRefreshService, scheduler, kubernetesClient, settings)

    verifyScannerScheduled()
    verifyWatcherLaunched()
    verify(secrets).inAnyNamespace()
    verify(secrets, never).inNamespace(anyString)
  }

  test("Secret finder sets up the scanner and watcher for a specified namespace") {
    val config = ConfigFactory.parseMap(configMap2.asJava)
    val settings = new Settings(config)
    val secretFinder = SecretFinder(tokenRefreshService, scheduler, kubernetesClient, settings)

    verifyScannerScheduled()
    verifyWatcherLaunched()
    verify(secrets).inNamespace("my-namespace")
    verify(secrets, never).inAnyNamespace()
  }

  test("Stopping the secret finder cancels the scanner and watcher") {
    val config = ConfigFactory.parseMap(configMap1.asJava)
    val settings = new Settings(config)
    val secretFinder = SecretFinder(tokenRefreshService, scheduler, kubernetesClient, settings)
    val scanner = captureScanner()
    secretFinder.stop()

    verify(cancellable).cancel()
    verify(watch).close()
  }

  test("Scanner sends the refresh service secrets to track ") {
    when(secretsWithLabel.list()).thenReturn(secretList)
    val secrets = List(secret1, secret2)
    when(secretList.getItems).thenReturn(secrets.asJava)
    val config = ConfigFactory.parseMap(configMap1.asJava)
    val settings = new Settings(config)
    val scanner = new SecretScanner(tokenRefreshService, kubernetesClient, settings)
    scanner.run()

    tokenRefreshServiceProbe.expectMsg(UpdateSecretsToTrack(secrets))
  }

  test("Watcher sends the refresh service new or deleted secret") {
    val config = ConfigFactory.parseMap(configMap1.asJava)
    val settings = new Settings(config)
    val watcher = new SecretWatcher(tokenRefreshService)

    watcher.eventReceived(Action.ADDED, secret1)
    tokenRefreshServiceProbe.expectMsg(StartRefresh(secret1))

    watcher.eventReceived(Action.DELETED, secret2)
    tokenRefreshServiceProbe.expectMsg(StopRefresh(secret2))

    watcher.eventReceived(Action.MODIFIED, secret1)  // Ignored.
    watcher.eventReceived(Action.ERROR, secret1)  // Ignored.
  }

  private def verifyScannerScheduled() = {
    val scanner = captureScanner()
    assert(scanner.getClass == classOf[SecretScanner])
  }

  private def captureScanner() = {
    val scannerCaptor: ArgumentCaptor[Runnable] = ArgumentCaptor.forClass(classOf[Runnable])
    verify(scheduler).schedule(
      Matchers.eq(Duration(SECRET_SCANNER_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS)),
      Matchers.eq(Duration(SECRET_SCANNER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)),
      scannerCaptor.capture())(
      any(classOf[ExecutionContext])
    )
    scannerCaptor.getValue
  }

  private def verifyWatcherLaunched() = {
    val watcher = captureWatcher()
    assert(watcher.getClass == classOf[SecretWatcher])
  }

  private def captureWatcher() = {
    val watcherCaptor: ArgumentCaptor[Watcher[Secret]] = ArgumentCaptor.forClass(
      classOf[Watcher[Secret]])
    verify(secretsWithLabel).watch(watcherCaptor.capture())
    watcherCaptor.getValue
  }
}
