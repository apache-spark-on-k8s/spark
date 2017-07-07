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
package org.apache.spark.deploy.kubernetes.submit

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

private[spark] trait PythonSubmissionResources {
  def sparkJars: Seq[String]
  def pySparkFiles: Array[String]
  def arguments: Array[String]
  def primaryPySparkResource(containerLocalizedFilesResolver: ContainerLocalizedFilesResolver)
    : String
  def driverPodWithPySparkEnvs(
    driverPodFileMounter: DriverPodKubernetesFileMounter,
    resolvedPrimaryPySparkResource: String,
    resolvedPySparkFiles: String,
    driverContainerName: String,
    driverPodBuilder: PodBuilder): Pod
}

private[spark] class PythonSubmissionResourcesImpl(
  private val mainAppResource: String,
  private val appArgs: Array[String] ) extends PythonSubmissionResources {

  private val pyFiles: Array[String] = {
    Option(appArgs(0)).map(a => mainAppResource +: a.split(","))
      .getOrElse(Array(mainAppResource))
  }

  override def sparkJars: Seq[String] = Seq.empty[String]

  override def pySparkFiles: Array[String] = pyFiles

  override def arguments: Array[String] = {
    pyFiles.toList match {
      case Nil => appArgs
      case a :: b => a match {
        case _ if a == mainAppResource && b == Nil => appArgs
        case _ => appArgs.drop(1)
      }
    }
  }
  override def primaryPySparkResource(
    containerLocalizedFilesResolver: ContainerLocalizedFilesResolver) : String =
      containerLocalizedFilesResolver.resolvePrimaryResourceFile()

  override def driverPodWithPySparkEnvs(
    driverPodFileMounter: DriverPodKubernetesFileMounter,
    resolvedPrimaryPySparkResource: String,
    resolvedPySparkFiles: String,
    driverContainerName: String,
    driverPodBuilder: PodBuilder) : Pod = {
      driverPodFileMounter
        .addPySparkFiles(
          resolvedPrimaryPySparkResource,
          resolvedPySparkFiles,
          driverContainerName,
          driverPodBuilder)
        .build()
    }
}
