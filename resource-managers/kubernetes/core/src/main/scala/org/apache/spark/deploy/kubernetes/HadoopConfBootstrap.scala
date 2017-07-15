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

import java.io.File

import io.fabric8.kubernetes.api.model.{ContainerBuilder, KeyToPathBuilder, PodBuilder}

import org.apache.spark.deploy.kubernetes.constants._

/**
 * This is separated out from the HadoopConf steps API because this component can be reused to
 * set up the hadoop-conf for executors as well.
 */
private[spark] trait HadoopConfBootstrap {
 /**
  * Bootstraps a main container with the ConfigMaps mounted as volumes and an ENV variable
  * pointing to the mounted file.
  */
  def bootstrapMainContainerAndVolumes(
    originalPodWithMainContainer: PodWithMainContainer)
  : PodWithMainContainer
}

private[spark] class HadoopConfBootstrapImpl(
  hadoopConfConfigMapName: String,
  hadoopConfigFiles: Array[File]) extends HadoopConfBootstrap {

  override def bootstrapMainContainerAndVolumes(
    originalPodWithMainContainer: PodWithMainContainer)
    : PodWithMainContainer = {
    import collection.JavaConverters._
    val fileContents = hadoopConfigFiles.map(file => (file.getPath, file.toString)).toMap
    val keyPaths = hadoopConfigFiles.map(file =>
      new KeyToPathBuilder().withKey(file.getPath).withPath(file.getAbsolutePath).build())
    val hadoopSupportedPod = new PodBuilder(originalPodWithMainContainer.pod)
      .editSpec()
        .addNewVolume()
          .withName(HADOOP_FILE_VOLUME)
            .withNewConfigMap()
              .withName(hadoopConfConfigMapName)
              .addAllToItems(keyPaths.toList.asJavaCollection)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    val mainContainerWithMountedHadoopConf = new ContainerBuilder(
      originalPodWithMainContainer.mainContainer)
      .addNewVolumeMount()
        .withName(HADOOP_FILE_VOLUME)
        .withMountPath(HADOOP_FILE_DIR)
        .endVolumeMount()
      .addNewEnv()
        .withName(HADOOP_CONF_DIR)
        .withValue(s"$HADOOP_FILE_DIR/$HADOOP_FILE_VOLUME")
        .endEnv()
      .build()
    PodWithMainContainer(
      hadoopSupportedPod,
      mainContainerWithMountedHadoopConf
    )
  }
}
