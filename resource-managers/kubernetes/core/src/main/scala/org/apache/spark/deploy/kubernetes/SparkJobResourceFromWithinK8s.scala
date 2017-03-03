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

import java.nio.file.{Files, Paths}

import io.fabric8.kubernetes.client.{Config, KubernetesClient}
import scala.util.{Failure, Success, Try}

import org.apache.spark.deploy.kubernetes.tpr.TPRCrudCalls

private[spark] class SparkJobResourceClientFromWithinK8s(
    client: KubernetesClient) extends TPRCrudCalls {

  private val protocol: String = "https://"

  // we can also get the host from the environment variable
  private val kubeHost: String = {
    val host = Try(sys.env("KUBERNETES_SERVICE_HOST")) match {
      case Success(h) => Option(h)
      case Failure(_) => None
    }
    host.map(h => h).getOrElse {
      // Log a warning just in case, but this should almost certainly never happen
      logWarning("Error while retrieving k8s host address")
      "127.0.0.1"
    }
  }

  // the port from the environment variable
  private val kubeHostPort: String = {
    val port = Try(sys.env("KUBERNETES_PORT_443_TCP_PORT")) match {
      case Success(p) => Option(p)
      case Failure(_) => None
    }
    port.map(p => p).getOrElse {
      // Log a warning just in case, but this should almost certainly never happen
      logWarning("Error while retrieving k8s host port")
      "8001"
    }
  }

  // Since this will be running inside a pod
  // we can access the pods token and use it with the Authorization header when
  // making rest calls to the k8s Api
  override protected val kubeToken: Option[String] = {
    val path = Paths.get(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)
    Try(new String(Files.readAllBytes(path))) match {
      case Success(some) => Option(some)
      case Failure(e: Throwable) => logError(s"${e.getMessage}")
        None
    }
  }

  override protected val k8sClient: KubernetesClient = client
  override protected val kubeMaster: String = s"$protocol$kubeHost:$kubeHostPort"
}

private[spark] class SparkJobResourceClientFromOutsideK8s(
    client: KubernetesClient) extends TPRCrudCalls {

  override protected val k8sClient: KubernetesClient = client
}
