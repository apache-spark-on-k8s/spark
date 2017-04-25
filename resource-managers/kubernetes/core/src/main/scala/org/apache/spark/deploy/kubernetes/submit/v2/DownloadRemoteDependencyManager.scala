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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.File

import io.fabric8.kubernetes.api.model.{ConfigMap, ContainerBuilder, EmptyDirVolumeSource, PodBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.apache.spark.util.Utils

private[spark] trait DownloadRemoteDependencyManager {

  def buildInitContainerConfigMap(): ConfigMap

  def configurePodToDownloadRemoteDependencies(
    initContainerConfigMap: ConfigMap,
    driverContainerName: String,
    originalPodSpec: PodBuilder): PodBuilder

  /**
   * Return the local classpath of the driver after all of its dependencies have been downloaded.
   */
  def resolveLocalClasspath(): Seq[String]
}

// TODO this is very similar to SubmittedDependencyManagerImpl. We should consider finding a way to
// consolidate the implementations.
private[spark] class DownloadRemoteDependencyManagerImpl(
    kubernetesAppId: String,
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    jarsDownloadPath: String,
    filesDownloadPath: String,
    initContainerImage: String) extends DownloadRemoteDependencyManager {

  private val jarsToDownload = KubernetesFileUtils.getOnlyRemoteFiles(sparkJars)
  private val filesToDownload = KubernetesFileUtils.getOnlyRemoteFiles(sparkFiles)

  override def buildInitContainerConfigMap(): ConfigMap = {
    val remoteJarsConf = if (jarsToDownload.nonEmpty) {
      Map(INIT_CONTAINER_REMOTE_JARS.key -> jarsToDownload.mkString(","))
    } else {
      Map.empty[String, String]
    }
    val remoteFilesConf = if (filesToDownload.nonEmpty) {
      Map(INIT_CONTAINER_REMOTE_FILES.key -> filesToDownload.mkString(","))
    } else {
      Map.empty[String, String]
    }
    val initContainerConfig = Map[String, String](
      DRIVER_REMOTE_JARS_DOWNLOAD_LOCATION.key -> jarsDownloadPath,
      DRIVER_REMOTE_FILES_DOWNLOAD_LOCATION.key -> filesDownloadPath) ++
      remoteJarsConf ++
      remoteFilesConf
    PropertiesConfigMapFromScalaMapBuilder.buildConfigMap(
      s"$kubernetesAppId-remote-files-download-init",
      INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY,
      initContainerConfig)
  }

  override def configurePodToDownloadRemoteDependencies(
      initContainerConfigMap: ConfigMap,
      driverContainerName: String,
      originalPodSpec: PodBuilder): PodBuilder = {
    val sharedVolumeMounts = Seq(
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_REMOTE_FILES_DOWNLOAD_JARS_VOLUME_NAME)
        .withMountPath(jarsDownloadPath)
        .build(),
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_REMOTE_FILES_DOWNLOAD_FILES_VOLUME_NAME)
        .withMountPath(filesDownloadPath)
        .build())
    val initContainer = new ContainerBuilder()
      .withName(INIT_CONTAINER_REMOTE_FILES_CONTAINER_NAME)
      .withArgs(INIT_CONTAINER_REMOTE_FILES_PROPERTIES_FILE_PATH)
      .addNewVolumeMount()
        .withName(INIT_CONTAINER_REMOTE_FILES_PROPERTIES_FILE_VOLUME)
        .withMountPath(INIT_CONTAINER_REMOTE_FILES_PROPERTIES_FILE_MOUNT_PATH)
        .endVolumeMount()
      .addToVolumeMounts(sharedVolumeMounts: _*)
      .withImage(initContainerImage)
      .withImagePullPolicy("IfNotPresent")
      .build()
    InitContainerUtil.appendInitContainer(originalPodSpec, initContainer)
      .editSpec()
        .addNewVolume()
          .withName(INIT_CONTAINER_REMOTE_FILES_PROPERTIES_FILE_VOLUME)
          .withNewConfigMap()
            .withName(initContainerConfigMap.getMetadata.getName)
            .addNewItem()
              .withKey(INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY)
              .withPath(INIT_CONTAINER_REMOTE_FILES_PROPERTIES_FILE_NAME)
              .endItem()
            .endConfigMap()
          .endVolume()
        .addNewVolume()
          .withName(INIT_CONTAINER_REMOTE_FILES_DOWNLOAD_JARS_VOLUME_NAME)
          .withEmptyDir(new EmptyDirVolumeSource())
          .endVolume()
        .addNewVolume()
          .withName(INIT_CONTAINER_REMOTE_FILES_DOWNLOAD_FILES_VOLUME_NAME)
          .withEmptyDir(new EmptyDirVolumeSource())
          .endVolume()
        .editMatchingContainer(new ContainerNameEqualityPredicate(driverContainerName))
          .addToVolumeMounts(sharedVolumeMounts: _*)
          .endContainer()
        .endSpec()
  }

  override def resolveLocalClasspath(): Seq[String] = {
    sparkJars.map { jar =>
      val jarUri = Utils.resolveURI(jar)
      val scheme = Option.apply(jarUri.getScheme).getOrElse("file")
      scheme match {
        case "file" | "local" => jarUri.getPath
        case _ =>
          val fileName = new File(jarUri.getPath).getName
          s"$jarsDownloadPath/$fileName"
      }
    }
  }
}
