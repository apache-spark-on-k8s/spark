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
package org.apache.spark.deploy.kubernetes

import java.io.File
import java.util.concurrent.CountDownLatch

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.rest.kubernetes._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class Client(
    sparkConf: SparkConf,
    mainClass: String,
    mainAppResource: String,
    appArgs: Array[String]) extends Logging {

  private val launchTime = System.currentTimeMillis
  private val appName = sparkConf.getOption("spark.app.name")
    .getOrElse("spark")
  private val kubernetesAppId = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val sparkFiles = sparkConf.getOption("spark.files")
    .map(_.split(","))
    .getOrElse(Array.empty[String])
  private val sparkJars = sparkConf.getOption("spark.jars")
    .map(_.split(","))
    .getOrElse(Array.empty[String])
  private val waitForAppCompletion: Boolean = sparkConf.get(WAIT_FOR_APP_COMPLETION)

  def run(): Unit = {
    logInfo(s"Starting application $kubernetesAppId in Kubernetes...")
    val submitterLocalFiles = KubernetesFileUtils.getOnlySubmitterLocalFiles(sparkFiles)
    val submitterLocalJars = KubernetesFileUtils.getOnlySubmitterLocalFiles(sparkJars)
    (submitterLocalFiles ++ submitterLocalJars).foreach { file =>
      if (!new File(Utils.resolveURI(file).getPath).isFile) {
        throw new SparkException(s"File $file does not exist or is a directory.")
      }
    }
    if (KubernetesFileUtils.isUriLocalFile(mainAppResource) &&
        !new File(Utils.resolveURI(mainAppResource).getPath).isFile) {
      throw new SparkException(s"Main app resource file $mainAppResource is not a file or" +
        s" is a directory.")
    }
    val kubernetesClientProvider = new SubmissionKubernetesClientProvider(sparkConf)
    val driverSslConfigurationProvider = new SslConfigurationProvider(sparkConf, kubernetesAppId)
    val driverPodKubernetesCredentials = new DriverPodKubernetesCredentialsProvider(sparkConf).get
    Utils.tryWithResource(kubernetesClientProvider.getKubernetesClient) { kubernetesClient =>
      val loggingInterval = if (waitForAppCompletion) sparkConf.get(REPORT_INTERVAL) else 0
      val driverPodCompletedLatch = new CountDownLatch(1)
      val loggingWatch = new LoggingPodStatusWatcher(driverPodCompletedLatch, kubernetesAppId,
        loggingInterval)
      Utils.tryWithResource(kubernetesClient
          .pods()
          .withName(kubernetesAppId)
          .watch(loggingWatch)) { _ =>
        val submitApplicationStep = new KubernetesDriverSubmitter(
          kubernetesAppId,
          appName,
          mainAppResource,
          mainClass,
          appArgs,
          submitterLocalFiles,
          submitterLocalJars,
          driverPodKubernetesCredentials)
        val kubernetesDriverLifecycle = new KubernetesDriverLifecycle(
          appName,
          kubernetesAppId,
          kubernetesClient,
          sparkConf,
          driverSslConfigurationProvider.getSslConfiguration(),
          submitApplicationStep)
        kubernetesDriverLifecycle.startKubernetesDriver()
        // wait if configured to do so
        if (waitForAppCompletion) {
          logInfo(s"Waiting for application $kubernetesAppId to finish...")
          driverPodCompletedLatch.await()
          logInfo(s"Application $kubernetesAppId finished.")
        } else {
          logInfo(s"Application $kubernetesAppId successfully launched.")
        }
      }
    }
  }
}

private[spark] object Client extends Logging {

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, s"Too few arguments. Usage: ${getClass.getName} <mainAppResource>" +
      s" <mainClass> [<application arguments>]")
    val mainAppResource = args(0)
    val mainClass = args(1)
    val appArgs = args.drop(2)
    val sparkConf = new SparkConf(true)
    new Client(
      mainAppResource = mainAppResource,
      mainClass = mainClass,
      sparkConf = sparkConf,
      appArgs = appArgs).run()
  }

}
