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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._

private[spark] trait DownloadRemoteDependencyManagerProvider {
  def getDownloadRemoteDependencyManager(
      kubernetesAppId: String,
      sparkJars: Seq[String],
      sparkFiles: Seq[String]): DownloadRemoteDependencyManager
}

private[spark] class DownloadRemoteDependencyManagerProviderImpl(
    sparkConf: SparkConf) extends DownloadRemoteDependencyManagerProvider {

  override def getDownloadRemoteDependencyManager(
      kubernetesAppId: String,
      sparkJars: Seq[String],
      sparkFiles: Seq[String]): DownloadRemoteDependencyManager = {
    new DownloadRemoteDependencyManagerImpl(
      kubernetesAppId,
      sparkJars,
      sparkFiles,
      sparkConf.get(REMOTE_JARS_DOWNLOAD_LOCATION),
      sparkConf.get(REMOTE_FILES_DOWNLOAD_LOCATION),
      sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE))
  }
}
