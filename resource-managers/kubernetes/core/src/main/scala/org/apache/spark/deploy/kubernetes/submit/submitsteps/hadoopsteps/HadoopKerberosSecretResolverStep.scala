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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.{KerberosConfBootstrapImpl, PodWithMainContainer}

 /**
  * Step that configures the ConfigMap + Volumes for the driver
  */
private[spark] class HadoopKerberosSecretResolverStep(
  submissionSparkConf: SparkConf,
  tokenLabelName: String) extends HadoopConfigurationStep {

  override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    val bootstrapKerberos = new KerberosConfBootstrapImpl(tokenLabelName)
    val withKerberosEnvPod = bootstrapKerberos.bootstrapMainContainerAndVolumes(
      PodWithMainContainer(
        hadoopConfigSpec.driverPod,
        hadoopConfigSpec.driverContainer))
    hadoopConfigSpec.copy(
      driverPod = withKerberosEnvPod.pod,
      driverContainer = withKerberosEnvPod.mainContainer)
  }
}
