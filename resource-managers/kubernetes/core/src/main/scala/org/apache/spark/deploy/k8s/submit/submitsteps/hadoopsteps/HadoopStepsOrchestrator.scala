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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{HadoopConfBootstrapImpl, HadoopUGIUtil, OptionRequirements}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.internal.Logging

 /**
  * Returns the complete ordered list of steps required to configure the hadoop configurations.
  */
private[spark] class HadoopStepsOrchestrator(
  namespace: String,
  hadoopConfigMapName: String,
  submissionSparkConf: SparkConf,
  hadoopConfDir: String) extends Logging{
   private val isKerberosEnabled = submissionSparkConf.get(KUBERNETES_KERBEROS_SUPPORT)
   private val maybePrincipal = submissionSparkConf.get(KUBERNETES_KERBEROS_PRINCIPAL)
   private val maybeKeytab = submissionSparkConf.get(KUBERNETES_KERBEROS_KEYTAB)
     .map(k => new File(k))
   private val maybeExistingSecret = submissionSparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME)
   private val maybeExistingSecretItemKey =
     submissionSparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY)
   private val maybeRenewerPrincipal =
     submissionSparkConf.get(KUBERNETES_KERBEROS_RENEWER_PRINCIPAL)
   private val hadoopConfigurationFiles = getHadoopConfFiles(hadoopConfDir)
   private val hadoopUGI = new HadoopUGIUtil
   logInfo(s"Hadoop Conf directory: $hadoopConfDir")

   require(maybeKeytab.forall( _ => isKerberosEnabled ),
     "You must enable Kerberos support if you are specifying a Kerberos Keytab")

   require(maybeExistingSecret.forall( _ => isKerberosEnabled ),
     "You must enable Kerberos support if you are specifying a Kerberos Secret")

   OptionRequirements.requireBothOrNeitherDefined(
     maybeKeytab,
     maybePrincipal,
    "If a Kerberos keytab is specified you must also specify a Kerberos principal",
     "If a Kerberos principal is specified you must also specify a Kerberos keytab")

   OptionRequirements.requireBothOrNeitherDefined(
     maybeExistingSecret,
     maybeExistingSecretItemKey,
     "If a secret storing a Kerberos Delegation Token is specified you must also" +
     " specify the label where the data is stored",
     "If a secret data item-key where the data of the Kerberos Delegation Token is specified" +
       " you must also specify the name of the secret")

  def getHadoopSteps(): Seq[HadoopConfigurationStep] = {
    val hadoopConfBootstrapImpl = new HadoopConfBootstrapImpl(
      hadoopConfigMapName,
      hadoopConfigurationFiles,
      hadoopUGI)
    val hadoopConfMounterStep = new HadoopConfMounterStep(
      hadoopConfigMapName,
      hadoopConfigurationFiles,
      hadoopConfBootstrapImpl,
      hadoopConfDir)
    val maybeKerberosStep =
      if (isKerberosEnabled) {
        maybeExistingSecret.map(existingSecretName => Some(new HadoopKerberosSecretResolverStep(
         submissionSparkConf,
          existingSecretName,
          maybeExistingSecretItemKey.get))).getOrElse(Some(
            new HadoopKerberosKeytabResolverStep(
              submissionSparkConf,
              maybePrincipal,
              maybeKeytab,
              maybeRenewerPrincipal,
              hadoopUGI)))
      } else {
        Option.empty[HadoopConfigurationStep]
      }
    Seq(hadoopConfMounterStep) ++ maybeKerberosStep.toSeq
  }

  private def getHadoopConfFiles(path: String) : Seq[File] = {
     def isFile(file: File) = if (file.isFile) Some(file) else None
     val dir = new File(path)
     if (dir.isDirectory) {
        dir.listFiles.flatMap { file => isFile(file) }.toSeq
     } else {
       Seq.empty[File]
     }
  }
}
