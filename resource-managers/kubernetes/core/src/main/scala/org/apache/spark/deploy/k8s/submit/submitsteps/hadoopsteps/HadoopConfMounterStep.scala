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
package org.apache.spark.deploy.k8s.submit.submitsteps.hadoopsteps

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.deploy.k8s.{HadoopConfBootstrap, PodWithMainContainer}
import org.apache.spark.deploy.k8s.constants._

 /**
  * This step is responsible for taking the contents from each file in
  * HADOOP_CONF_DIR, grabbing its contents as a string and storing each of them
  * as a key-value pair in a configmap. Each key-value pair will be stored
  * as a file, via Volume Mounts, later. The HADOOP_CONF_DIR_LOC is passed into the
  * SchedulerBackend via sparkConf.
  */
private[spark] class HadoopConfMounterStep(
    hadoopConfigMapName: String,
    hadoopConfigurationFiles: Seq[File],
    hadoopConfBootstrapConf: HadoopConfBootstrap,
    hadoopConfDir: Option[String],
    noNeedUploadHadoopConf: Boolean = false)
  extends HadoopConfigurationStep {

   override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    val bootstrappedPodAndMainContainer =
      hadoopConfBootstrapConf.bootstrapMainContainerAndVolumes(
        PodWithMainContainer(
          hadoopConfigSpec.driverPod,
          hadoopConfigSpec.driverContainer))
     hadoopConfigSpec.copy(
       driverPod = bootstrappedPodAndMainContainer.pod,
       driverContainer = bootstrappedPodAndMainContainer.mainContainer,
       configMapProperties =
         hadoopConfigurationFiles.filter(_ => !noNeedUploadHadoopConf).map(file =>
           (file.toPath.getFileName.toString, Files.toString(file, Charsets.UTF_8))).toMap,
       additionalDriverSparkConf = hadoopConfDir.filter(_ => !noNeedUploadHadoopConf)
         .foldLeft(hadoopConfigSpec.additionalDriverSparkConf)((sparkConf, conf) =>
           sparkConf ++ Map(HADOOP_CONF_DIR_LOC -> conf))
     )
  }
}
