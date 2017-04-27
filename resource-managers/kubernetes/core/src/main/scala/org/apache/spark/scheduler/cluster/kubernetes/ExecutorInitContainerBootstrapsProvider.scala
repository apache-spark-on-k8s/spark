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
package org.apache.spark.scheduler.cluster.kubernetes

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.v2.{InitContainerSecretConfiguration, SparkPodInitContainerBootstrap, SparkPodInitContainerBootstrapImpl}
import org.apache.spark.internal.config.OptionalConfigEntry

/**
 * Adds init-containers to the executor pods that downloads resources from remote locations and the
 * resource staging server, if applicable.
 */
private[spark] trait ExecutorInitContainerBootstrapsProvider {
  def getInitContainerBootstraps: Iterable[SparkPodInitContainerBootstrap]

}

private[spark] class ExecutorInitContainerBootstrapsProviderImpl(sparkConf: SparkConf)
    extends ExecutorInitContainerBootstrapsProvider {
  import ExecutorInitContainerBootstrapsProvider._
  private val submittedJarsDownloadPath = sparkConf.get(SUBMITTED_JARS_DOWNLOAD_LOCATION)
  private val submittedFilesDownloadPath = sparkConf.get(SUBMITTED_FILES_DOWNLOAD_LOCATION)
  private val submittedFilesInitContainerConfigMap = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP)
  private val submittedFilesInitContainerConfigMapKey = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY)
  private val submittedFilesInitContainerSecret = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET)
  private val submittedFilesInitContainerSecretDir = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET_DIR)

  requireAllOrNoneDefined(Seq(
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP,
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY,
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET,
    EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET_DIR),
    submittedFilesInitContainerConfigMap,
    submittedFilesInitContainerConfigMapKey,
    submittedFilesInitContainerSecret,
    submittedFilesInitContainerSecretDir)

  private val remoteJarsDownloadPath = sparkConf.get(REMOTE_JARS_DOWNLOAD_LOCATION)
  private val remoteFilesDownloadPath = sparkConf.get(REMOTE_FILES_DOWNLOAD_LOCATION)
  private val remoteFilesInitContainerConfigMap = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP)
  private val remoteFilesInitContainerConfigMapKey = sparkConf.get(
    EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY)

  requireAllOrNoneDefined(Seq(
    EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP,
    EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY),
    remoteFilesInitContainerConfigMap,
    remoteFilesInitContainerConfigMapKey)

  private val initContainerImage = sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE)

  override def getInitContainerBootstraps: Iterable[SparkPodInitContainerBootstrap] = {
    val submittedFileBootstrap = for {
      configMap <- submittedFilesInitContainerConfigMap
      configMapKey <- submittedFilesInitContainerConfigMapKey
      secret <- submittedFilesInitContainerSecret
      secretDir <- submittedFilesInitContainerSecretDir
    } yield {
      new SparkPodInitContainerBootstrapImpl(
        INIT_CONTAINER_SUBMITTED_FILES_SUFFIX,
        configMap,
        configMapKey,
        initContainerImage,
        submittedJarsDownloadPath,
        submittedFilesDownloadPath,
        Some(InitContainerSecretConfiguration(secretName = secret, secretMountPath = secretDir)))
    }

    val remoteFilesBootstrap = for {
      configMap <- remoteFilesInitContainerConfigMap
      configMapKey <- remoteFilesInitContainerConfigMapKey
    } yield {
      new SparkPodInitContainerBootstrapImpl(
        INIT_CONTAINER_REMOTE_FILES_SUFFIX,
        configMap,
        configMapKey,
        initContainerImage,
        remoteJarsDownloadPath,
        remoteFilesDownloadPath)
    }
    submittedFileBootstrap.toSeq ++ remoteFilesBootstrap.toSeq
  }
}

private object ExecutorInitContainerBootstrapsProvider {
  def requireAllOrNoneDefined(
      configEntries: Seq[OptionalConfigEntry[_]], opts: Option[_]*): Unit = {
    if (opts.exists(_.isDefined)) {
      val errorMessage = s"If any of the following configurations are defined, all of the others" +
        s" must also be provided: ${configEntries.map(_.key).mkString(",")}"
      require(opts.forall(_.isDefined), errorMessage)
    }
  }
}
