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

import io.fabric8.kubernetes.api.model.{Container, PodBuilder}

import org.apache.spark.deploy.kubernetes.constants._

 /**
  * Trait that is responsible for providing full file-paths dynamically after
  * the filesDownloadPath has been defined. The file-names are then stored in the
  * environmental variables in the driver-pod.
  */
private[spark] trait DriverPodKubernetesFileMounter {
  def addPySparkFiles(primaryFile: String, pySparkFiles: String,
    mainContainerName: String, originalPodSpec: PodBuilder) : PodBuilder
}

private[spark] class DriverPodKubernetesFileMounterImpl()
  extends DriverPodKubernetesFileMounter {
  override def addPySparkFiles(
        primaryFile: String,
        pySparkFiles: String,
        mainContainerName: String,
        originalPodSpec: PodBuilder): PodBuilder = {

    originalPodSpec
      .editSpec()
        .editMatchingContainer(new ContainerNameEqualityPredicate(mainContainerName))
          .addNewEnv()
            .withName(ENV_PYSPARK_PRIMARY)
            .withValue(primaryFile)
          .endEnv()
          .addNewEnv()
            .withName(ENV_PYSPARK_FILES)
            .withValue(pySparkFiles)
          .endEnv()
        .endContainer()
      .endSpec()
  }
}
