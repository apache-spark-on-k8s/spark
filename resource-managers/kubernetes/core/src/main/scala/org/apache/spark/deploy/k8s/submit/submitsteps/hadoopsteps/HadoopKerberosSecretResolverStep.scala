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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KerberosTokenConfBootstrapImpl, PodWithMainContainer}
import org.apache.spark.deploy.k8s.constants._

 /**
  * This step assumes that you have already done all the heavy lifting in retrieving a
  * delegation token and storing the following data in a secret before running this job.
  * This step requires that you just specify the secret name and data item-key corresponding
  * to the data where the delegation token is stored.
  */
private[spark] class HadoopKerberosSecretResolverStep(
  submissionSparkConf: SparkConf,
  tokenSecretName: String,
  tokenItemKeyName: String) extends HadoopConfigurationStep {

  override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    val bootstrapKerberos = new KerberosTokenConfBootstrapImpl(
      tokenSecretName,
      tokenItemKeyName,
      UserGroupInformation.getCurrentUser.getShortUserName)
    val withKerberosEnvPod = bootstrapKerberos.bootstrapMainContainerAndVolumes(
      PodWithMainContainer(
        hadoopConfigSpec.driverPod,
        hadoopConfigSpec.driverContainer))
    hadoopConfigSpec.copy(
      driverPod = withKerberosEnvPod.pod,
      driverContainer = withKerberosEnvPod.mainContainer,
      additionalDriverSparkConf =
        hadoopConfigSpec.additionalDriverSparkConf ++ Map(
          HADOOP_KERBEROS_CONF_ITEM_KEY -> tokenItemKeyName,
          HADOOP_KERBEROS_CONF_SECRET -> tokenSecretName),
      dtSecret = None,
      dtSecretName = tokenSecretName,
      dtSecretItemKey = tokenItemKeyName)
  }
}
