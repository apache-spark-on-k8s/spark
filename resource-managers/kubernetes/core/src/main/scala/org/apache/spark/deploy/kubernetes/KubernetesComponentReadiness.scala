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

import java.io.Closeable
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.internal.readiness.{Readiness, ReadinessWatcher}
import scala.concurrent.TimeoutException

import org.apache.spark.SparkException
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Waits for a set of Kubernetes components to become ready. Note that this is similar to
 * KubernetesClient.resourceList(...).waitUntilReady(). However, that implementation
 * currently has a race condition because it does not initialize the watchers before the
 * resources are created. Thus there can be a window where the resource becomes "ready" before
 * the client begins listening for the event for the component to become ready. The readiness
 * event is then missed and the client ends up hanging indefinitely.
 *
 * This class adjusts the approach by constructing the watchers for the components upon the
 * instantiation of this class, but blocking on the readiness can be deferred until after the
 * components are ready. So an example usage of this class would be:
 *
 * <pre>
 * {@code
 * Utils.tryWithResource(new KubernetesComponentReadiness(...)) { readiness =>
 *    kubernetesClient.pods().create(pod)
 *    readiness.await()
 * }
 * }
 * </pre>
 */
private[spark] class KubernetesComponentReadiness(
  kubernetesClient: KubernetesClient,
  components: Iterable[HasMetadata]) extends Closeable {

  private val readinessApplicableComponents = components.filter(Readiness.isReadinessApplicable)
  private val readinessWatchers = readinessApplicableComponents.map { component =>
    (component, new ReadinessWatcher(component))
  }
  private val readinessWatches = readinessWatchers.map { watcherAndComponent =>
    kubernetesClient.resource(watcherAndComponent._1).watch(watcherAndComponent._2)
  }

  private val readinessExecutor = ThreadUtils.newDaemonFixedThreadPool(
    readinessWatches.size,
    "kubernetes-component-readiness")

  def await(waitTimeoutSeconds: Long): Unit = {
    val readinessSignals = new CountDownLatch(readinessApplicableComponents.size)
    val readinessFutures = readinessWatchers.map { componentAndWatcher =>
      val future = readinessExecutor.submit(new Runnable {
        override def run(): Unit = {
          // Wait for infinite time here - the timeout will be handled by the
          // wait on the count down latch.
          componentAndWatcher._2.await(Long.MaxValue, TimeUnit.DAYS)
          readinessSignals.countDown()
        }
      })
      (componentAndWatcher._1, future)
    }
    try {
      readinessSignals.await(waitTimeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: TimeoutException =>
        val notReadyComponents = readinessFutures
          .filterNot(_._2.isDone)
          .map(_._1)
          .map(component => {
            s"[ Name: ${component.getMetadata.getName} | Kind: ${component.getKind} ]"
          })
        throw new SparkException(s"The following components were not ready in" +
          s" $waitTimeoutSeconds seconds: ${notReadyComponents.mkString(",")}", e)
    }
  }

  override def close(): Unit = {
    readinessWatches.foreach { watch =>
      Utils.tryLogNonFatalError {
        watch.close()
      }
    }
    readinessExecutor.shutdown()
  }
}
