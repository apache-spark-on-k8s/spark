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

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.{ActorRef, ActorSystem, Scheduler}
import com.typesafe.config.{Config, ConfigFactory}
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}
import org.apache.log4j.{Level, Logger}

private class Server(injector: Injector) {

  private val actorSystem = injector.newActorSystem()
  private val kubernetesClient = injector.newKubernetesClient()
  private val settings = injector.newSettings()
  private var secretFinder : Option[SecretFinder] = None

  def start(): Unit = {
    val refreshService = injector.newTokenRefreshService(actorSystem, kubernetesClient, settings)
    secretFinder = Some(injector.newSecretFinder(refreshService, kubernetesClient,
      actorSystem.scheduler, settings))
  }

  def join() : Unit = {
    // scalastyle:off awaitready
    Await.ready(actorSystem.whenTerminated, Duration.Inf)
    // scalastyle:on awaitready
  }

  def stop(): Unit = {
    actorSystem.terminate()
    secretFinder.foreach(_.stop())
    kubernetesClient.close()
  }
}

private class Injector {

  def newActorSystem(): ActorSystem = ActorSystem("TokenRefreshServer")

  def newKubernetesClient(): KubernetesClient = new DefaultKubernetesClient()

  def newSettings(): Settings = new Settings()

  def newTokenRefreshService(actorSystem: ActorSystem, client: KubernetesClient,
                             settings: Settings): ActorRef =
    TokenRefreshService(actorSystem, client, settings)

  def newSecretFinder(refreshService: ActorRef, client: KubernetesClient, scheduler: Scheduler,
          settings: Settings): SecretFinder =
    SecretFinder(refreshService, scheduler, client, settings)
}

private class Settings(config: Config = ConfigFactory.load) {

  private val configKeyPrefix = "hadoop-token-refresh-server"

  val refreshServerKerberosPrincipal : String = config.getString(
    s"$configKeyPrefix.kerberosPrincipal")

  val shouldScanAllNamespaces : Boolean = config.getBoolean(s"$configKeyPrefix.scanAllNamespaces")

  val namespaceToScan : String = config.getString(s"$configKeyPrefix.namespaceToScan")
}

private class CommandLine(args: List[String]) {

  var logLevel: Level = Level.WARN

  parse(args)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO
      parse(tail)
    case ("--debug" | "-d") :: tail =>
      logLevel = Level.DEBUG
      parse(tail)
    case unknown if unknown.nonEmpty =>
        usage()
        throw new IllegalArgumentException(s"Got an unknown argument: $unknown")
    case _ =>
  }

  private def usage(): Unit = {
    // scalastyle:off println
    println("Usage: TokenRefreshServer [--verbose | -v] [--debug | -d]")
    // scalastyle:on println
  }
}

private class Launcher(parsedArgs: CommandLine, server: Server) {

  def launch(): Unit = {
    Logger.getRootLogger.setLevel(parsedArgs.logLevel)
    try {
      server.start()
      server.join()
    } finally {
      server.stop()
    }
  }
}

/*
 * TODO: Support REST endpoint for checking status of tokens.
 */
object TokenRefreshServer {

  def main(args: Array[String]): Unit = {
    val parsedArgs = new CommandLine(args.toList)
    val server = new Server(new Injector)
    val launcher = new Launcher(parsedArgs, server)
    launcher.launch()
  }
}
