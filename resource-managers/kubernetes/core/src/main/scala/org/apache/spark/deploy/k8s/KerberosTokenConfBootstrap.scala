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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}

import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.internal.Logging


 /**
  * This is separated out from the HadoopConf steps API because this component can be reused to
  * mounted the DT secret for executors as well.
  */
private[spark] trait KerberosTokenConfBootstrap {
  // Bootstraps a main container with the Secret mounted as volumes and an ENV variable
  // pointing to the mounted file containing the DT for Secure HDFS interaction
  def bootstrapMainContainerAndVolumes(
  originalPodWithMainContainer: PodWithMainContainer)
  : PodWithMainContainer
}

private[spark] class KerberosTokenConfBootstrapImpl(
  secretName: String,
  secretItemKey: String,
  userName: String) extends KerberosTokenConfBootstrap with Logging{


  override def bootstrapMainContainerAndVolumes(
  originalPodWithMainContainer: PodWithMainContainer)
  : PodWithMainContainer = {
    logInfo("Mounting HDFS DT from Secret for Secure HDFS")
    val dtMountedPod = new PodBuilder(originalPodWithMainContainer.pod)
      .editOrNewSpec()
        .addNewVolume()
          .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
          .withNewSecret()
            .withSecretName(secretName)
            .endSecret()
          .endVolume()
        .endSpec()
      .build()
    val mainContainerWithMountedKerberos = new ContainerBuilder(
      originalPodWithMainContainer.mainContainer)
      .addNewVolumeMount()
        .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
        .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
        .endVolumeMount()
      .addNewEnv()
        .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
        .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$secretItemKey")
        .endEnv()
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(userName)
        .endEnv()
      .build()
    originalPodWithMainContainer.copy(
      pod = dtMountedPod,
      mainContainer = mainContainerWithMountedKerberos)
  }
}
