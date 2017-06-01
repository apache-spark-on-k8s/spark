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

package org.apache.spark.deploy.kubernetes

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.kubernetes.RegisterKubernetesApp
import org.apache.spark.network.util.TransportConf
import org.apache.spark.scheduler.cluster.kubernetes.DriverPodKubernetesClientProvider


/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Kubernetes.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[spark] class KubernetesShuffleBlockHandler (
  sparkConf: SparkConf,
  transportConf: TransportConf)
  extends ExternalShuffleBlockHandler(transportConf, null) with Logging {

  private var connectedApps = mutable.Set.empty[String]
  private var shuffleWatch: Option[Watch] = ShuffleWatcher()
  private val CONNECTED_APPS_LOCK = new Object

  protected override def handleMessage(
    message: BlockTransferMessage,
    client: TransportClient,
    callback: RpcResponseCallback): Unit = {
      message match {
        case RegisterDriverParam(appId) =>
          val address = client.getSocketAddress
          logInfo(s"Received registration request from app $appId (remote address $address).")
          CONNECTED_APPS_LOCK.synchronized {
            if (connectedApps.contains(appId)) {
              logWarning(s"Received a registration request from app $appId, but it was already " +
                s"registered")
            }
            connectedApps += appId
          }
          callback.onSuccess(ByteBuffer.allocate(0))
        case _ => super.handleMessage(message, client, callback)
      }
  }

  private def ShuffleWatcher(): Option[Watch] = {
    try {
      val kubernetesClient = new DriverPodKubernetesClientProvider(sparkConf).get
      Some(kubernetesClient
        .pods()
        .withLabels(Map(SPARK_ROLE_LABEL -> "driver").asJava)
        .watch(new Watcher[Pod] {
          override def eventReceived(action: Watcher.Action, p: Pod): Unit = {
            action match {
              case Action.DELETED | Action.ERROR =>
                val labels = p.getMetadata.getLabels
                if (labels.containsKey(SPARK_APP_ID_LABEL)) {
                  val appId = labels.get(SPARK_APP_ID_LABEL)
                  CONNECTED_APPS_LOCK.synchronized {
                    if (connectedApps.contains(appId)) {
                      connectedApps -= appId
                      applicationRemoved(appId, true)
                    }
                  }
                }
              case Action.ADDED | Action.MODIFIED =>
            }
          }

          override def onClose(e: KubernetesClientException): Unit = {}
        }))
    } catch {
      case throwable: Throwable =>
        logWarning(s"Shuffle service cannot access Kubernetes. " +
          s"Orphaned file cleanup is disabled.", throwable)
        None
    }
  }

  /** An extractor object for matching [[RegisterKubernetesApp]] message. */
  private object RegisterDriverParam {
    def unapply(r: RegisterKubernetesApp): Option[(String)] =
      Some(r.getAppId)
  }
}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to associate with. This allows the shuffle service to detect when a driver is terminated
 * and can clean up the associated shuffle files.
 */
private[spark] class KubernetesExternalShuffleService(conf: SparkConf,
  securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  protected override def newShuffleBlockHandler(
    tConf: TransportConf): ExternalShuffleBlockHandler = {
    new KubernetesShuffleBlockHandler(conf, tConf)
  }
}

private[spark] object KubernetesExternalShuffleService extends Logging {
  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new KubernetesExternalShuffleService(conf, sm))
  }
}


