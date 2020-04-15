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

import scala.concurrent.Future
import scala.collection.JavaConverters._
import akka.actor.{ActorRef, ActorSystem, Scheduler}
import com.typesafe.config.ConfigFactory
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.log4j.Level
import org.mockito.{Answers, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doReturn, never, verify, when}
import org.scalatest.{BeforeAndAfter, FunSuite}


class TokenRefreshServerSuite extends FunSuite with BeforeAndAfter {

  private val configKeyPrefix = "hadoop-token-refresh-server"
  private val config = ConfigFactory.parseMap(
    Map(s"$configKeyPrefix.kerberosPrincipal" -> "my-principal",
      s"$configKeyPrefix.scanAllNamespaces" -> true,
      s"$configKeyPrefix.namespaceToScan" -> "my-namespace").asJava)
  private val settings = new Settings(config)
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var injector: Injector = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var actorSystem: ActorSystem = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var scheduler: Scheduler = _
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private var actorSystemAwaitFuture: Future[Unit] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var kubernetesClient: KubernetesClient = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var tokenRefreshServiceActorRef: ActorRef = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretFinder: SecretFinder = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var mockServer: Server = _
  private var server : Server = _

  before {
    MockitoAnnotations.initMocks(this)
    when(injector.newActorSystem()).thenReturn(actorSystem)
    when(actorSystem.scheduler).thenReturn(scheduler)
    when(injector.newKubernetesClient()).thenReturn(kubernetesClient)
    when(injector.newSettings()).thenReturn(settings)
    when(injector.newTokenRefreshService(actorSystem, kubernetesClient, settings))
      .thenReturn(tokenRefreshServiceActorRef)
    when(injector.newSecretFinder(tokenRefreshServiceActorRef, kubernetesClient, scheduler,
        settings))
      .thenReturn(secretFinder)
    doReturn(actorSystemAwaitFuture).when(actorSystem).whenTerminated
    server = new Server(injector)
  }

  test("The token refresh server starts the refresh service actor and secret finder") {
    server.start()
    verify(injector).newTokenRefreshService(actorSystem, kubernetesClient, settings)
    verify(injector).newSecretFinder(tokenRefreshServiceActorRef, kubernetesClient, scheduler,
      settings)
    verify(actorSystem, never()).whenTerminated
  }

  test("The token refresh server waits until the refresh service finishes") {
    server.start()
    server.join()
    verify(actorSystem).whenTerminated
  }

  test("The token refresh server stops the refresh service and secret finder") {
    server.start()
    server.stop()
    verify(actorSystem).terminate()
    verify(kubernetesClient).close()
    verify(secretFinder).stop()
  }

  test("The command line parses properly") {
    var parsedArgs = new CommandLine(List())
    assert(parsedArgs.logLevel == Level.WARN)

    parsedArgs = new CommandLine(List("-v"))
    assert(parsedArgs.logLevel == Level.INFO)
    parsedArgs = new CommandLine(List("--verbose"))
    assert(parsedArgs.logLevel == Level.INFO)

    parsedArgs = new CommandLine(List("-d"))
    assert(parsedArgs.logLevel == Level.DEBUG)
    parsedArgs = new CommandLine(List("--debug"))
    assert(parsedArgs.logLevel == Level.DEBUG)
  }

  test("Unknown command line arguments throws") {
    intercept[IllegalArgumentException] {
      new CommandLine(List(""))
    }
    intercept[IllegalArgumentException] {
      new CommandLine(List("-f"))
    }
    intercept[IllegalArgumentException] {
      new CommandLine(List("--unknown"))
    }
  }

  test("The server launches properly") {
    val launcher = new Launcher(new CommandLine(List()), mockServer)
    launcher.launch()

    verify(mockServer).start()
    verify(mockServer).join()
    verify(mockServer).stop()
  }

  test("The server stops properly upon error") {
    when(mockServer.stop()).thenThrow(new RuntimeException)
    val launcher = new Launcher(new CommandLine(List()), mockServer)
    intercept[RuntimeException] {
      launcher.launch()
    }

    verify(mockServer).start()
    verify(mockServer).join()
    verify(mockServer).stop()
  }
}
