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

import io.fabric8.kubernetes.api.model.{Pod, Secret, Service}
import io.fabric8.kubernetes.api.model.extensions.Ingress
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.util.{ShutdownHookManager, Utils}

private[spark] class KubernetesComponentCleaner(kubernetesClient: KubernetesClient) {
  private val LOCK = new Object
  private val registeredPods = mutable.HashMap.empty[String, Pod]
  private val registeredServices = mutable.HashMap.empty[String, Service]
  private val registeredSecrets = mutable.HashMap.empty[String, Secret]
  private val registeredIngresses = mutable.HashMap.empty[String, Ingress]

  ShutdownHookManager.addShutdownHook(() => purgeAllRegisteredComponentsFromKubernetes())

  def registerOrUpdatePod(pod: Pod): Unit = LOCK.synchronized {
    registeredPods.put(pod.getMetadata.getName, pod)
  }

  def unregisterPod(pod: Pod): Unit = LOCK.synchronized {
    registeredPods.remove(pod.getMetadata.getName)
  }

  def registerOrUpdateService(service: Service): Unit = LOCK.synchronized {
    registeredServices.put(service.getMetadata.getName, service)
  }

  def unregisterService(service: Service): Unit = LOCK.synchronized {
    registeredServices.remove(service.getMetadata.getName)
  }

  def registerOrUpdateSecret(secret: Secret): Unit = LOCK.synchronized {
    registeredSecrets.put(secret.getMetadata.getName, secret)
  }

  def unregisterSecret(secret: Secret): Unit = LOCK.synchronized {
    registeredSecrets.remove(secret.getMetadata.getName)
  }

  def registerOrUpdateIngress(ingress: Ingress): Unit = LOCK.synchronized {
    registeredIngresses.put(ingress.getMetadata.getName, ingress)
  }

  def unregisterIngress(ingress: Ingress): Unit = LOCK.synchronized {
    registeredIngresses.remove(ingress.getMetadata.getName)
  }

  def purgeAllRegisteredComponentsFromKubernetes(): Unit = LOCK.synchronized {
    registeredPods.values.foreach { pod =>
      Utils.tryLogNonFatalError {
        kubernetesClient.pods().delete(pod)
      }
    }
    registeredPods.clear()
    registeredServices.values.foreach { service =>
      Utils.tryLogNonFatalError {
        kubernetesClient.services().delete(service)
      }
    }
    registeredServices.clear()
    registeredSecrets.values.foreach { secret =>
      Utils.tryLogNonFatalError {
        kubernetesClient.secrets().delete(secret)
      }
    }
    registeredSecrets.clear()
    registeredIngresses.values.foreach { ingress =>
      Utils.tryLogNonFatalError {
        kubernetesClient.extensions().ingresses().delete(ingress)
      }
    }
    registeredIngresses.clear()
  }
}
