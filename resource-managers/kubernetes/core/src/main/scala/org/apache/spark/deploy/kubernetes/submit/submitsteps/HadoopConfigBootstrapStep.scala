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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import java.io.StringWriter
import java.util.Properties

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, HasMetadata}
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps.{HadoopConfigSpec, HadoopConfigurationStep}

 /**
  * Configures the driverSpec that bootstraps dependencies into the driver pod.
  */
private[spark] class HadoopConfigBootstrapStep(
  hadoopConfigurationSteps: Seq[HadoopConfigurationStep], kubernetesResourceNamePrefix: String)
  extends DriverConfigurationStep {
  private val hadoopConfigMapName = s"$kubernetesResourceNamePrefix-hadoop-config"

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    import scala.collection.JavaConverters._
    var currentHadoopSpec = HadoopConfigSpec(
      driverPod = driverSpec.driverPod,
      driverContainer = driverSpec.driverContainer,
      configMapProperties = Map.empty[String, String])
    for (nextStep <- hadoopConfigurationSteps) {
      currentHadoopSpec = nextStep.configureContainers(currentHadoopSpec)
    }
    val configMap =
      new ConfigMapBuilder()
        .withNewMetadata()
        .withName(hadoopConfigMapName)
        .endMetadata()
          .addToData(currentHadoopSpec.configMapProperties.asJava)
      .build()
    val executorSparkConf = driverSpec.driverSparkConf.clone()
      .set(HADOOP_CONFIG_MAP_SPARK_CONF_NAME, hadoopConfigMapName)
    driverSpec.copy(
      driverPod = currentHadoopSpec.driverPod,
      driverContainer = currentHadoopSpec.driverContainer,
      driverSparkConf = executorSparkConf,
      otherKubernetesResources = Seq(configMap)
      )
  }
}
