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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging

/**
 * Helper utility for creating the error message upon failing to submit
 * the Kubernetes-based application. Primarily just broken out of
 * KubernetesDriverLifecycle for readability / testing but is logically still a
 * lifecycle operation.
 */
private[spark] class SubmissionFailureErrorMessageProvider(
    driverSubmitTimeoutSecs: Long,
    kubernetesClient: KubernetesClient,
    driverPodModel: Pod) extends Logging {
  def getSubmitFailedErrorMessage: String = {
    Try {
      kubernetesClient.resource(driverPodModel).fromServer().get()
    }.recoverWith {
      // Make sure we capture any error that comes from trying to get the driver pod
      // But there's no point to proceed further in constructing a comprehensive error
      // message, so just propagate the exception immediately after logging it.
      case throwable: Throwable =>
        logError(s"Timed out while waiting $driverSubmitTimeoutSecs seconds for the" +
          " driver pod to start, but an error occurred while fetching the driver" +
          " pod's details.", throwable)
        throw throwable
    }.map(serverDriverPod => {
      val topLevelMessage = s"The driver pod with name ${serverDriverPod.getMetadata.getName}" +
        s" in namespace ${serverDriverPod.getMetadata.getNamespace} was not ready in" +
        s" $driverSubmitTimeoutSecs seconds."
      val podStatusPhase = if (serverDriverPod.getStatus.getPhase != null) {
        s"Latest phase from the pod is: ${serverDriverPod.getStatus.getPhase}"
      } else {
        "The pod had no final phase."
      }
      val podStatusMessage = if (serverDriverPod.getStatus.getMessage != null) {
        s"Latest message from the pod is: ${serverDriverPod.getStatus.getMessage}"
      } else {
        "The pod had no final message."
      }
      val failedDriverContainerStatusString = serverDriverPod.getStatus
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
    }).getOrElse {
      s"Timed out while waiting $driverSubmitTimeoutSecs" +
        " seconds for the driver pod to start. Unfortunately, in attempting to fetch" +
        " the latest state of the pod, another error was thrown. Check the logs for" +
        " the error that was thrown in looking up the driver pod."
    }
  }
}
