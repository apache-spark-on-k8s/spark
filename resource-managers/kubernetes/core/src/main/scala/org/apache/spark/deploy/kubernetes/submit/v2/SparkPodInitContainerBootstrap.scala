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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EmptyDirVolumeSource, PodBuilder, VolumeBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.deploy.kubernetes.constants._

private[spark] case class InitContainerSecretConfiguration(
    secretName: String,
    secretMountPath: String)

private[spark] trait SparkPodInitContainerBootstrap {

  /**
   * Bootstraps an init-container that downloads dependencies to be used by a main container.
   * Note that this primarily assumes that the init-container's configuration is being provided
   * by a Config Map that was installed by some other component; that is, the implementation
   * here makes no assumptions about how the init-container is specifically configured. For
   * example, this class is unaware if the init-container is fetching remote dependencies or if
   * it is fetching dependencies from a resource staging server.
   */
  def bootstrapInitContainerAndVolumes(
      mainContainerName: String,
      originalPodSpec: PodBuilder): PodBuilder
}

private[spark] class SparkPodInitContainerBootstrapImpl(
    initContainerComponentsSuffix: String,
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String,
    initContainerImage: String,
    jarsDownloadPath: String,
    filesDownloadPath: String,
    initContainerSecret: Option[InitContainerSecretConfiguration] = None)
    extends SparkPodInitContainerBootstrap {
  override def bootstrapInitContainerAndVolumes(
      mainContainerName: String,
      originalPodSpec: PodBuilder): PodBuilder = {
    val jarsVolumeName = s"$INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME-$initContainerComponentsSuffix"
    val filesVolumeName =
      s"$INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME-$initContainerComponentsSuffix"
    val sharedVolumeMounts = Seq[VolumeMount](
      new VolumeMountBuilder()
        .withName(jarsVolumeName)
        .withMountPath(jarsDownloadPath)
        .build(),
      new VolumeMountBuilder()
        .withName(filesVolumeName)
        .withMountPath(filesDownloadPath)
        .build())

    val propertiesFileDirectory = s"/etc/spark-init/init-$initContainerComponentsSuffix"
    val propertiesFilePath = s"$propertiesFileDirectory/$INIT_CONTAINER_PROPERTIES_FILE_NAME"
    val initContainerSecretVolume = initContainerSecret.map { secretNameAndMountPath =>
      new VolumeBuilder()
        .withName(s"$INIT_CONTAINER_SECRET_VOLUME_NAME-$initContainerComponentsSuffix")
        .withNewSecret()
          .withSecretName(secretNameAndMountPath.secretName)
          .endSecret()
        .build()
    }
    val initContainerSecretVolumeMount = initContainerSecret.map { secretNameAndMountPath =>
      new VolumeMountBuilder()
        .withName(s"$INIT_CONTAINER_SECRET_VOLUME_NAME-$initContainerComponentsSuffix")
        .withMountPath(secretNameAndMountPath.secretMountPath)
        .build()
    }
    val initContainer = new ContainerBuilder()
      .withName(s"spark-init-$initContainerComponentsSuffix")
      .withImage(initContainerImage)
      .withImagePullPolicy("IfNotPresent")
      .addNewVolumeMount()
        .withName(s"$INIT_CONTAINER_PROPERTIES_FILE_VOLUME-$initContainerComponentsSuffix")
        .withMountPath(propertiesFileDirectory)
        .endVolumeMount()
      .addToVolumeMounts(initContainerSecretVolumeMount.toSeq: _*)
      .addToVolumeMounts(sharedVolumeMounts: _*)
      .addToArgs(propertiesFilePath)
      .build()
    InitContainerUtil.appendInitContainer(originalPodSpec, initContainer)
      .editSpec()
      .addNewVolume()
        .withName(s"$INIT_CONTAINER_PROPERTIES_FILE_VOLUME-$initContainerComponentsSuffix")
        .withNewConfigMap()
          .withName(initContainerConfigMapName)
          .addNewItem()
            .withKey(initContainerConfigMapKey)
            .withPath(INIT_CONTAINER_PROPERTIES_FILE_NAME)
            .endItem()
          .endConfigMap()
        .endVolume()
      .addNewVolume()
        .withName(jarsVolumeName)
        .withEmptyDir(new EmptyDirVolumeSource())
        .endVolume()
      .addNewVolume()
        .withName(filesVolumeName)
        .withEmptyDir(new EmptyDirVolumeSource())
        .endVolume()
      .addToVolumes(initContainerSecretVolume.toSeq: _*)
      .editMatchingContainer(new ContainerNameEqualityPredicate(mainContainerName))
        .addToVolumeMounts(sharedVolumeMounts: _*)
        .endContainer()
      .endSpec()
  }
}
