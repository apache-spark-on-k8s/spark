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
package org.apache.spark.deploy.k8s.integrationtest.backend.minikube

import java.util.UUID

import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.spark.deploy.k8s.config.KUBERNETES_TEST_DOCKER_TAG_SYSTEM_PROPERTY
import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend
import org.apache.spark.deploy.k8s.integrationtest.docker.KubernetesSuiteDockerManager

private[spark] object MinikubeTestBackend extends IntegrationTestBackend {

  private var defaultClient: DefaultKubernetesClient = _
  private val userProvidedDockerImageTag = Option(
      System.getProperty(KUBERNETES_TEST_DOCKER_TAG_SYSTEM_PROPERTY))
  private val resolvedDockerImageTag =
      userProvidedDockerImageTag.getOrElse(UUID.randomUUID().toString.replaceAll("-", ""))
  private val dockerManager = new KubernetesSuiteDockerManager(
      Minikube.getDockerEnv, resolvedDockerImageTag)

  override def initialize(): Unit = {
    val minikubeStatus = Minikube.getMinikubeStatus
    require(minikubeStatus == MinikubeStatus.RUNNING,
        s"Minikube must be running before integration tests can execute. Current status" +
            s" is: $minikubeStatus")
    if (userProvidedDockerImageTag.isEmpty) {
       dockerManager.buildSparkDockerImages()
    }
    defaultClient = Minikube.getKubernetesClient
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    if (userProvidedDockerImageTag.isEmpty) {
      dockerManager.deleteImages()
    }
  }

  override def getKubernetesClient(): DefaultKubernetesClient = {
    defaultClient
  }

  override def dockerImageTag(): String = resolvedDockerImageTag
}
