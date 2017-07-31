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

import scala.collection.JavaConverters._
import scala.util.Try

import io.fabric8.kubernetes.api.model.SecretBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

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
   private var originalCredentials: Credentials = _
   private var dfs : FileSystem = _
   private var renewer: String = _
   private var renewedCredentials: Credentials = _
   private var renewedTokens: Iterable[Token[_ <: TokenIdentifier]] = _
   override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(submissionSparkConf)
    logInfo(s"Hadoop Configuration: ${hadoopConf.toString}")
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
    // It is necessary to run as jobUserUGI because logged in user != Current User
    jobUserUGI.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        logInfo(s"Retrieved Job User UGI: $jobUserUGI")
        originalCredentials = jobUserUGI.getCredentials
        logInfo(s"Original tokens: ${originalCredentials.toString}")
        logInfo(s"All tokens: ${originalCredentials.getAllTokens}")
        logInfo(s"All secret keys: ${originalCredentials.getAllSecretKeys}")
        dfs = FileSystem.get(hadoopConf)
        // This is not necessary with [Spark-20328] since we would be using
        // Spark core providers to handle delegation token renewal
        renewer = jobUserUGI.getShortUserName
        logInfo(s"Renewer is: $renewer")
        renewedCredentials = new Credentials(originalCredentials)
        dfs.addDelegationTokens(renewer, renewedCredentials)
        renewedTokens = renewedCredentials.getAllTokens.asScala
        logInfo(s"Renewed tokens: ${renewedCredentials.toString}")
        logInfo(s"All renewed tokens: ${renewedTokens.mkString(",")}")
        logInfo(s"All renewed secret keys: ${renewedCredentials.getAllSecretKeys}")
        null
      }})
    if (renewedTokens.isEmpty) logError("Did not obtain any Delegation Tokens")
    val data = serialize(renewedCredentials)
    val renewalTime = getTokenRenewalInterval(renewedTokens, hadoopConf).getOrElse(Long.MaxValue)
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

  // Functions that should be in Core with Rebase to 2.3
  @deprecated("Moved to core in 2.2", "2.2")
  private def getTokenRenewalInterval(
    renewedTokens: Iterable[Token[_ <: TokenIdentifier]],
    hadoopConf: Configuration): Option[Long] = {
      val renewIntervals = renewedTokens.filter {
        _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]}
        .flatMap { token =>
        Try {
          val newExpiration = token.renew(hadoopConf)
          val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          val interval = newExpiration - identifier.getIssueDate
          logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
          interval
        }.toOption}
      if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
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
