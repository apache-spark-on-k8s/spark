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

import java.lang
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.{ActorRef, Scheduler}
import io.fabric8.kubernetes.api.model.{Secret, SecretList}
import io.fabric8.kubernetes.client._
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable

import org.apache.spark.security.kubernetes.constants._


private trait SecretSelection {

  def selectSecrets(kubernetesClient: KubernetesClient, settings: Settings):
          FilterWatchListDeletable[Secret, SecretList, lang.Boolean, Watch, Watcher[Secret]] = {
    val selector = kubernetesClient.secrets()
    val namespacedSelector = if (settings.shouldScanAllNamespaces) {
      selector.inAnyNamespace()
    } else {
      selector.inNamespace(settings.namespaceToScan)
    }
    namespacedSelector
      .withLabel(SECRET_LABEL_KEY_REFRESH_HADOOP_TOKENS, SECRET_LABEL_VALUE_REFRESH_HADOOP_TOKENS)
  }
}

private class SecretFinder(refreshService: ActorRef,
                           scheduler: Scheduler,
                           kubernetesClient: KubernetesClient,
                           settings: Settings) extends SecretSelection {

  private val cancellable = scheduler.schedule(
    Duration(SECRET_SCANNER_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS),
    Duration(SECRET_SCANNER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS),
    new SecretScanner(refreshService, kubernetesClient, settings))
  private val watched = selectSecrets(kubernetesClient, settings)
    .watch(new SecretWatcher(refreshService))

  def stop(): Unit = {
    cancellable.cancel()
    watched.close()
  }
}

private class SecretScanner(refreshService: ActorRef,
                            kubernetesClient: KubernetesClient,
                            settings: Settings) extends Runnable with SecretSelection with Logging {

  override def run(): Unit = {
    val secrets = selectSecrets(kubernetesClient, settings).list.getItems.asScala.toList
    logInfo(s"Scanned ${secrets.map(_.getMetadata.getSelfLink).mkString(", ")}")
    refreshService ! UpdateSecretsToTrack(secrets)
  }
}

private class SecretWatcher(refreshService: ActorRef) extends Watcher[Secret] with Logging {

  override def eventReceived(action: Action, secret: Secret): Unit = {
    action match {
      case Action.ADDED =>
        logInfo(s"Found ${secret.getMetadata.getSelfLink} added")
        refreshService ! StartRefresh(secret)
      case Action.DELETED =>
        logInfo(s"Found ${secret.getMetadata.getSelfLink} deleted")
        refreshService ! StopRefresh(secret)
      case _ =>  // Do nothing
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    // Do nothing
  }
}

private object SecretFinder {

  def apply(refreshService: ActorRef, scheduler: Scheduler, client: KubernetesClient,
            settings: Settings): SecretFinder =
    new SecretFinder(refreshService, scheduler, client, settings)
}
