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

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, EnvVar, EnvVarBuilder, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

// Note that we use ConfigMaps here as opposed to uploading through the Driver
// REST server after the REST server has started. The reason for this is that
// the REST server may itself need the Hadoop configuration files to download
// remote dependencies.
private[spark] case class MountedHadoopConfiguration(
  confDirVolume: Volume,
  confDirVolumeMount: VolumeMount,
  confDirEnv: EnvVar,
  confMap: ConfigMap)

private[spark] case class MountedHadoopConfigurations(
  hadoopConfDir: MountedHadoopConfiguration,
  yarnConfDir: MountedHadoopConfiguration)

private[spark] class MountedHadoopConfigurationsProvider(kubernetesAppId: String)
    extends Logging {

  def get(): MountedHadoopConfigurations = {
    MountedHadoopConfigurations(
      getMountedHadoopConfiguration(
        confEnvVar = "HADOOP_CONF_DIR",
        configMapName = s"$kubernetesAppId-hadoopConfDir",
        mountPath = "/etc/hadoop/hadoopConfDir"),
      getMountedHadoopConfiguration(
        confEnvVar = "YARN_CONF_DIR",
        configMapName = s"$kubernetesAppId-yarnConfDir",
        mountPath = "/etc/hadoop/yarnConfDir")
    )
  }

  private def getMountedHadoopConfiguration(
      confEnvVar: String,
      configMapName: String,
      mountPath: String): MountedHadoopConfiguration = {
    val confFilesContents: Map[String, String] = sys.env.get(confEnvVar)
      .map(new File(_))
      .filter(_.isDirectory)
      .map(dir => (dir, dir.listFiles()))
      .filterNot { files =>
        val isNull = files._2 == null
        if (isNull) {
          logWarning(s"Failed to list files from $confEnvVar at ${files._1.getAbsolutePath}")
        }
        isNull
      }.flatMap(_._2.toSeq)
      .filter { file =>
        val isFile = file.isFile
        // We could theoretically make a way to load directories, but this isn't supported by YARN
        // and simplifies the logic greatly. Might be worth considering in a future iteration.
        if (!isFile) {
          logWarning(s"File ${file.getAbsolutePath} loaded from $confEnvVar is not a file; it" +
            s" will not be loaded into the driver.")
        }
        isFile
      }.map(file => (file.getName, Files.toString(file, Charsets.UTF_8)))
      .toMap
    val confConfigMap = new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .withData(confFilesContents.asJava)
      .build()
    val confDirVolume = new VolumeBuilder()
      .withName(s"$configMapName-volume")
      .withNewConfigMap()
        .withName(confConfigMap.getMetadata.getName)
        .endConfigMap()
      .build()
    val confDirVolumeMount = new VolumeMountBuilder()
      .withName(confDirVolume.getName)
      .withMountPath(mountPath)
      .build()
    val confDirEnvVar = new EnvVarBuilder()
      .withName(confEnvVar)
      .withValue(mountPath)
      .build()
    MountedHadoopConfiguration(confDirVolume, confDirVolumeMount, confDirEnvVar, confConfigMap)
  }
}
