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

import java.io._
import java.security.PrivilegedExceptionAction

import io.fabric8.kubernetes.api.model.SecretBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.{KerberosConfBootstrapImpl, PodWithMainContainer}
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging

 /**
  * Step that configures the ConfigMap + Volumes for the driver
  */
private[spark] class HadoopKerberosKeytabResolverStep(
  submissionSparkConf: SparkConf,
  maybePrincipal: Option[String],
  maybeKeytab: Option[File]) extends HadoopConfigurationStep with Logging{

  override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    // FIXME: Pass down hadoopConf so you can call sc.hadoopConfiguration
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(submissionSparkConf)
    if (!UserGroupInformation.isSecurityEnabled) logError("Hadoop not configuration with Kerberos")
    val maybeJobUserUGI =
      for {
        principal <- maybePrincipal
        keytab <- maybeKeytab
      } yield {
        // Not necessary with [Spark-16742]
        // Reliant on [Spark-20328] for changing to YARN principal
        submissionSparkConf.set("spark.yarn.principal", principal)
        submissionSparkConf.set("spark.yarn.keytab", keytab.toURI.toString)
        logInfo("Logged into KDC with keytab using Job User UGI")
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principal,
          keytab.toURI.toString)
      }
    // In the case that keytab is not specified we will read from Local Ticket Cache
    val jobUserUGI = maybeJobUserUGI.getOrElse(UserGroupInformation.getCurrentUser)
    val credentials: Credentials = jobUserUGI.getCredentials
    val credentialsManager = newHadoopTokenManager(submissionSparkConf, hadoopConf)
    var renewalTime = Long.MaxValue
    jobUserUGI.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        renewalTime = Math.min(
          obtainCredentials(credentialsManager, hadoopConf, credentials),
          renewalTime)
        null
      }
    })
    if (credentials.getAllTokens.isEmpty) logError("Did not obtain any Delegation Tokens")
    val data = serialize(credentials)
    val delegationToken = HDFSDelegationToken(data, renewalTime)
    val initialTokenLabelName = s"$KERBEROS_SECRET_LABEL_PREFIX-1-$renewalTime"
    logInfo(s"Storing dt in $initialTokenLabelName")
    val secretDT =
      new SecretBuilder()
        .withNewMetadata()
          .withName(HADOOP_KERBEROS_SECRET_NAME)
          .endMetadata()
          .addToData(initialTokenLabelName, Base64.encodeBase64String(delegationToken.bytes))
      .build()
    val bootstrapKerberos = new KerberosConfBootstrapImpl(initialTokenLabelName)
    val withKerberosEnvPod = bootstrapKerberos.bootstrapMainContainerAndVolumes(
      PodWithMainContainer(
        hadoopConfigSpec.driverPod,
        hadoopConfigSpec.driverContainer))
    hadoopConfigSpec.copy(
      additionalDriverSparkConf =
        hadoopConfigSpec.additionalDriverSparkConf ++ Map(
          KERBEROS_SPARK_CONF_NAME -> initialTokenLabelName),
      driverPod = withKerberosEnvPod.pod,
      driverContainer = withKerberosEnvPod.mainContainer,
      dtSecret = Some(secretDT))
  }

  // Functions that should be in SparkHadoopUtil with Rebase to 2.2
  @deprecated("Moved to core in 2.2", "2.2")
  private def obtainCredentials(instance: Any, args: AnyRef*): Long = {
    val method = Class
      .forName("org.apache.spark.deploy.yarn.security.ConfigurableCredentialManager")
      .getMethod("obtainCredentials", classOf[Configuration], classOf[Configuration])
    method.setAccessible(true)
    method.invoke(instance, args: _*).asInstanceOf[Long]
  }
   @deprecated("Moved to core in 2.2", "2.2")
   // This method will instead be using HadoopDelegationTokenManager from Spark 2.2
   private def newHadoopTokenManager(args: AnyRef*): Any = {
     val constructor = Class
       .forName("org.apache.spark.deploy.yarn.security.ConfigurableCredentialManager")
       .getConstructor(classOf[SparkConf], classOf[Configuration])
     constructor.setAccessible(true)
     constructor.newInstance(args: _*)
   }
  @deprecated("Moved to core in 2.2", "2.2")
  private def serialize(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  @deprecated("Moved to core in 2.2", "2.2")
  private def deserialize(tokenBytes: Array[Byte]): Credentials = {
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(tokenBytes)))
    creds
  }
}
