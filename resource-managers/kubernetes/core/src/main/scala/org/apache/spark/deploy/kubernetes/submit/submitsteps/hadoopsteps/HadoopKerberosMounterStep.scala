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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.internal.Logging

// import org.apache.spark.deploy.security.HadoopDelegationTokenManager

private[spark] case class DelegationToken(
  principle: String,
  bytes: Array[Byte],
  renewal: Long)

 /**
  * This class is responsible for Hadoop DT renewal
  * TODO: THIS IS BLOCKED BY SPARK 2.2 REBASE
  */
private[spark] class HadoopKerberosMounterStep(
   submissionSparkConf: SparkConf)
  extends HadoopConfigurationStep with Logging {

   private val maybePrincipal = submissionSparkConf.get(KUBERNETES_KERBEROS_PRINCIPAL)
   private val maybeKeytab = submissionSparkConf.get(KUBERNETES_KERBEROS_KEYTAB).map(
     k => new File(k))

   override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
     val hadoopConf = SparkHadoopUtil.get.newConfiguration(submissionSparkConf)
     if (!UserGroupInformation.isSecurityEnabled) logError("Hadoop not configuration with Kerberos")
     for {
       principal <- maybePrincipal
       keytab <- maybeKeytab
     } yield {
       submissionSparkConf.set("spark.yarn.principal", principal)
       submissionSparkConf.set("spark.yarn.keytab", keytab.toURI.toString)
     }
     hadoopConfigSpec
   }
 }
