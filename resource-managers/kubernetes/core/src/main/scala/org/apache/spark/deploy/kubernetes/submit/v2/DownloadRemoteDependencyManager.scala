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

import io.fabric8.kubernetes.api.model.ConfigMap

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.apache.spark.util.Utils

private[spark] trait DownloadRemoteDependencyManager {

  def buildInitContainerConfigMap(): ConfigMap

  def getInitContainerBootstrap(initContainerConfigMap: ConfigMap): SparkPodInitContainerBootstrap

  /**
   * Return the local classpath of the driver after all of its dependencies have been downloaded.
   */
  def resolveLocalClasspath(): Seq[String]

  def configureExecutorsToFetchRemoteDependencies(
      sparkConf: SparkConf, initContainerConfigMap: ConfigMap): SparkConf
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
      REMOTE_JARS_DOWNLOAD_LOCATION.key -> jarsDownloadPath,
      REMOTE_FILES_DOWNLOAD_LOCATION.key -> filesDownloadPath) ++
      remoteJarsConf ++
      remoteFilesConf
    PropertiesConfigMapFromScalaMapBuilder.buildConfigMap(
      s"$kubernetesAppId-remote-files-download-init",
      INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY,
      initContainerConfig)
  }

  override def getInitContainerBootstrap(initContainerConfigMap: ConfigMap)
      : SparkPodInitContainerBootstrap = {
    new SparkPodInitContainerBootstrapImpl(
      INIT_CONTAINER_REMOTE_FILES_SUFFIX,
      initContainerConfigMap.getMetadata.getName,
      INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY,
      initContainerImage,
      jarsDownloadPath,
      filesDownloadPath)
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

  override def configureExecutorsToFetchRemoteDependencies(
      sparkConf: SparkConf, initContainerConfigMap: ConfigMap): SparkConf = {
    sparkConf.clone()
      .set(EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP,
        initContainerConfigMap.getMetadata.getName)
      .set(EXECUTOR_INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY,
        INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY)
  }
}
