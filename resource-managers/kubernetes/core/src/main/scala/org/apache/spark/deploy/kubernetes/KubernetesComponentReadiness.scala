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

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.{Endpoints, Pod, Service}
import io.fabric8.kubernetes.api.model.extensions.Ingress
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private class ReadinessSignalWatcher[T](
    readinessCondition: (T => Boolean),
    readinessTimeoutSecs: Long,
    signal: Option[SettableFuture[Boolean]] = Some(SettableFuture.create[Boolean]))
  extends Watcher[T] with Logging {

  override def eventReceived(action: Action, resource: T): Unit = {
    if ((action == Action.ADDED || action == Action.MODIFIED)
        && readinessCondition(resource)) {
      signal.foreach(_.set(true))
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    logDebug("Watch has been closed", cause)
  }

  def waitForReadyComponent(): Unit = signal.foreach(_.get(readinessTimeoutSecs, TimeUnit.SECONDS))
}

private[spark] object KubernetesComponentReadiness extends Logging {

  /**
   * Execute a block of code and then wait for Kubernetes components with the given
   * name to be ready.
   */
  def launchComponentsThenWaitForReadiness(
      kubernetesClient: KubernetesClient,
      nameOfAllKubernetesComponents: String,
      readinessTimeoutSecs: Long,
      createPod: (() => Pod),
      createService: (() => Service),
      createIngress: Option[(() => Ingress)]): (Pod, Service, Option[Ingress]) = {
    val driverPodReadinessWatcher = new ReadinessSignalWatcher[Pod](
      isDriverPodReady, readinessTimeoutSecs)
    val driverServiceReadinessWatcher = new ReadinessSignalWatcher[Service](
      isDriverServiceReady, readinessTimeoutSecs)
    val driverEndpointsReadinessWatcher = new ReadinessSignalWatcher[Endpoints](
      isDriverEndpointsReady, readinessTimeoutSecs)
    val driverIngressReadinessWatcher = new ReadinessSignalWatcher[Ingress](
      isDriverIngressReady,
      readinessTimeoutSecs,
      createIngress.map(_ => SettableFuture.create[Boolean]))

    Utils.tryWithResource(kubernetesClient
        .pods()
        .withName(nameOfAllKubernetesComponents)
        .watch(driverPodReadinessWatcher)) { _ =>
      Utils.tryWithResource(kubernetesClient
          .services()
          .withName(nameOfAllKubernetesComponents)
          .watch(driverServiceReadinessWatcher)) { _ =>
        Utils.tryWithResource(kubernetesClient
            .endpoints()
            .withName(nameOfAllKubernetesComponents)
            .watch(driverEndpointsReadinessWatcher)) { _ =>
          Utils.tryWithResource(kubernetesClient
              .extensions()
              .ingresses()
              .withName(nameOfAllKubernetesComponents)
              .watch(driverIngressReadinessWatcher)) { _ =>
            val driverPod = createPod()
            val driverService = createService()
            val driverIngress = createIngress.map(_.apply)
            try {
              driverPodReadinessWatcher.waitForReadyComponent()
              logInfo("Driver pod successfully created in Kubernetes cluster.")
            } catch {
              case e: Throwable =>
                val finalErrorMessage: String = buildSubmitPodFailedErrorMessage(
                    kubernetesClient,
                    nameOfAllKubernetesComponents,
                    readinessTimeoutSecs, e)
                logError(finalErrorMessage, e)
                throw new SparkException(finalErrorMessage, e)
            }
            try {
              driverServiceReadinessWatcher.waitForReadyComponent()
              logInfo("Driver service created successfully in Kubernetes.")
            } catch {
              case e: Throwable =>
                throw new SparkException(s"The driver service was not ready" +
                  s" in $readinessTimeoutSecs seconds.", e)
            }
            try {
              driverEndpointsReadinessWatcher.waitForReadyComponent()
              logInfo("Driver endpoints ready to receive application submission")
            } catch {
              case e: Throwable =>
                throw new SparkException(s"The driver service endpoint was not ready" +
                  s" in $readinessTimeoutSecs seconds.", e)
            }
            try {
              driverIngressReadinessWatcher.waitForReadyComponent()
              createIngress.foreach(_ => logInfo("Driver ingress ready to proxy application" +
                " submission request to driver service"))
            } catch {
                case e: Throwable =>
                  throw new SparkException(s"The driver ingress was not ready" +
                    s" in $readinessTimeoutSecs seconds.", e)
            }
            (driverPod, driverService, driverIngress)
          }
        }
      }
    }
  }

  private def buildSubmitPodFailedErrorMessage(
      kubernetesClient: KubernetesClient,
      podName: String,
      readinessTimeoutSecs: Long,
      e: Throwable): String = {
    val driverPod = try {
      kubernetesClient.pods().withName(podName).get()
    } catch {
      case throwable: Throwable =>
        logError(s"Timed out while waiting $readinessTimeoutSecs seconds for the" +
          " driver pod to start, but an error occurred while fetching the driver" +
          " pod's details.", throwable)
        throw new SparkException(s"Timed out while waiting $readinessTimeoutSecs" +
          " seconds for the driver pod to start. Unfortunately, in attempting to fetch" +
          " the latest state of the pod, another error was thrown. Check the logs for" +
          " the error that was thrown in looking up the driver pod.", e)
    }
    val topLevelMessage = s"The driver pod with name ${driverPod.getMetadata.getName}" +
      s" in namespace ${driverPod.getMetadata.getNamespace} was not ready in" +
      s" $readinessTimeoutSecs seconds."
    val podStatusPhase = if (driverPod.getStatus.getPhase != null) {
      s"Latest phase from the pod is: ${driverPod.getStatus.getPhase}"
    } else {
      "The pod had no final phase."
    }
    val podStatusMessage = if (driverPod.getStatus.getMessage != null) {
      s"Latest message from the pod is: ${driverPod.getStatus.getMessage}"
    } else {
      "The pod had no final message."
    }
    val failedDriverContainerStatusString = driverPod.getStatus
      .getContainerStatuses
      .asScala
      .find(_.getName == DRIVER_CONTAINER_NAME)
      .map(status => {
        val lastState = status.getState
        if (lastState.getRunning != null) {
          "Driver container last state: Running\n" +
            s"Driver container started at: ${lastState.getRunning.getStartedAt}"
        } else if (lastState.getWaiting != null) {
          "Driver container last state: Waiting\n" +
            s"Driver container wait reason: ${lastState.getWaiting.getReason}\n" +
            s"Driver container message: ${lastState.getWaiting.getMessage}\n"
        } else if (lastState.getTerminated != null) {
          "Driver container last state: Terminated\n" +
            s"Driver container started at: ${lastState.getTerminated.getStartedAt}\n" +
            s"Driver container finished at: ${lastState.getTerminated.getFinishedAt}\n" +
            s"Driver container exit reason: ${lastState.getTerminated.getReason}\n" +
            s"Driver container exit code: ${lastState.getTerminated.getExitCode}\n" +
            s"Driver container message: ${lastState.getTerminated.getMessage}"
        } else {
          "Driver container last state: Unknown"
        }
      }).getOrElse("The driver container wasn't found in the pod; expected to find" +
      s" container with name $DRIVER_CONTAINER_NAME")
    s"$topLevelMessage\n" +
      s"$podStatusPhase\n" +
      s"$podStatusMessage\n\n$failedDriverContainerStatusString"
  }

  private def isDriverPodReady(pod: Pod): Boolean = {
    pod.getStatus.getPhase == "Running" &&
      pod.getStatus.getContainerStatuses.asScala.nonEmpty &&
      pod.getStatus.getContainerStatuses.asScala.exists(status =>
        status.getName == DRIVER_CONTAINER_NAME && status.getReady)
  }

  private def isDriverServiceReady(service: Service): Boolean = true

  private def isDriverEndpointsReady(endpoints: Endpoints): Boolean = {
    endpoints.getSubsets.asScala.nonEmpty &&
      endpoints.getSubsets.asScala.exists(_.getAddresses.asScala.nonEmpty)
  }

  private def isDriverIngressReady(ingress: Ingress): Boolean = {
    ingress.getStatus.getLoadBalancer.getIngress.asScala.nonEmpty
  }
}
