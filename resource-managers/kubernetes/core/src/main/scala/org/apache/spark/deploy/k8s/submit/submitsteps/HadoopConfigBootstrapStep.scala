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
package org.apache.spark.deploy.k8s.submit.submitsteps

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.submitsteps.hadoopsteps.{HadoopConfigSpec, HadoopConfigurationStep}

 /**
  * This class configures the driverSpec with hadoop configuration logic which includes
  * volume mounts, config maps, and environment variable manipulation. The steps are
  * resolved with the orchestrator and they are run modifying the HadoopSpec with each
  * step. The final HadoopSpec's contents will be appended to the driverSpec.
  */
private[spark] class HadoopConfigBootstrapStep(
  hadoopConfigurationSteps: Seq[HadoopConfigurationStep],
  hadoopConfigMapName: String,
  noNeedUploadHadoopConf: Boolean = false)
  extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    var currentHadoopSpec = HadoopConfigSpec(
      driverPod = driverSpec.driverPod,
      driverContainer = driverSpec.driverContainer,
      configMapProperties = Map.empty[String, String],
      additionalDriverSparkConf = Map.empty[String, String],
      dtSecret = None,
      dtSecretName = KERBEROS_DELEGEGATION_TOKEN_SECRET_NAME,
      dtSecretItemKey = "")
    for (nextStep <- hadoopConfigurationSteps) {
      currentHadoopSpec = nextStep.configureContainers(currentHadoopSpec)
    }
    val configMap = if (noNeedUploadHadoopConf) {
      Option.empty[HasMetadata]
    } else {
      Some(new ConfigMapBuilder()
        .withNewMetadata()
        .withName(hadoopConfigMapName)
        .endMetadata()
        .addToData(currentHadoopSpec.configMapProperties.asJava)
        .build())
    }
    val driverSparkConfWithExecutorSetup = driverSpec.driverSparkConf.clone()
      .set(HADOOP_CONFIG_MAP_SPARK_CONF_NAME, hadoopConfigMapName)
      .setAll(currentHadoopSpec.additionalDriverSparkConf)
    driverSpec.copy(
      driverPod = currentHadoopSpec.driverPod,
      driverContainer = currentHadoopSpec.driverContainer,
      driverSparkConf = driverSparkConfWithExecutorSetup,
      otherKubernetesResources =
        driverSpec.otherKubernetesResources ++
        configMap.toSeq ++ currentHadoopSpec.dtSecret.toSeq
      )
  }
}
