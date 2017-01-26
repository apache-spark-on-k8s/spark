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
import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

/**
 * A monitor for a running Kubernetes application, logging on state change and interval.
 *
 * @param client Kubernetes client
 * @param appId
 * @param interval ms between each state request
 */
private[kubernetes] class PodStateMonitor(client: DefaultKubernetesClient,
                      appId: String,
                      interval: Long) extends Logging {

  /**
   * Log the state of the application until it finishes, either successfully or due to a
   * failure, logging status throughout and on every state change.
   *
   * When the application finishes, returns its final state, either "Succeeded" or "Failed".
   */
  def monitorToCompletion(): String = {

    var previousPhase: String = null

    while (true) {
      Thread.sleep(interval)

      val podState = requestCurrentPodState()
      val phase = podState.getStatus().getPhase()

      // log a short message every interval, plus full details on every state change
      logInfo(s"Application status for $appId (phase: $phase)")
      if (previousPhase != phase) {
        logInfo("Phase changed, new state: " + podState)
      }

      // terminal state -- return
      if (phase == "Succeeded" || phase == "Failed") {
        return phase
      }

      previousPhase = phase
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }

  private def requestCurrentPodState(): Pod = {
    client.pods().withName(appId).get()
  }
}
