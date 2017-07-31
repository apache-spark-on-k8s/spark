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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.HadoopConfBootstrapImpl
import org.apache.spark.deploy.kubernetes.config._


 /**
  * Returns the complete ordered list of steps required to configure the hadoop configurations.
  */
private[spark] class HadoopStepsOrchestrator(
  namespace: String,
  hadoopConfigMapName: String,
  submissionSparkConf: SparkConf,
  hadoopConfDir: Option[String]) {
  private val maybeKerberosSupport = submissionSparkConf.get(KUBERNETES_KERBEROS_SUPPORT)
  private val maybePrincipal = submissionSparkConf.get(KUBERNETES_KERBEROS_PRINCIPAL)
  private val maybeKeytab = submissionSparkConf.get(KUBERNETES_KERBEROS_KEYTAB)
    .map(k => new File(k))
  private val maybeExistingSecret = submissionSparkConf.get(KUBERNETES_KERBEROS_DT_SECRET)
  private val hadoopConfigurationFiles = hadoopConfDir.map(conf => getHadoopConfFiles(conf))
     .getOrElse(Seq.empty[File])

  def getHadoopSteps(): Seq[HadoopConfigurationStep] = {
    val hadoopConfBootstrapImpl = new HadoopConfBootstrapImpl(
      hadoopConfigMapName,
      hadoopConfigurationFiles)
    val hadoopConfMounterStep = new HadoopConfMounterStep(
      hadoopConfigMapName,
      hadoopConfigurationFiles,
      hadoopConfBootstrapImpl,
      hadoopConfDir)
    val maybeKerberosStep =
      if (maybeKerberosSupport) {
        maybeExistingSecret.map(secretLabel => Some(new HadoopKerberosSecretResolverStep(
         submissionSparkConf,
         secretLabel))).getOrElse(Some(
            new HadoopKerberosKeytabResolverStep(
              submissionSparkConf,
              maybePrincipal,
              maybeKeytab)))
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
