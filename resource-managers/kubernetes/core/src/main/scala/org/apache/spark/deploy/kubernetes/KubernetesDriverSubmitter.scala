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

import com.google.common.io.{BaseEncoding, Files}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.{AppResource, ContainerAppResource, KubernetesCreateSubmissionRequest, KubernetesCredentials, RemoteAppResource, UploadedAppResource}
import org.apache.spark.deploy.rest.kubernetes.{CompressionUtils, HttpClientUtil, KubernetesSparkRestApi}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class KubernetesDriverSubmitter(
    kubernetesAppId: String,
    kubernetesAppName: String,
    mainAppResource: String,
    mainClass: String,
    appArgs: Array[String],
    submitterLocalFiles: Iterable[String],
    submitterLocalJars: Iterable[String],
    driverPodKubernetesCredentials: KubernetesCredentials) extends Logging {
  def submitApplicationToDriverServer(
      driverSubmissionServerUris: Set[String],
      submissionSecret: String,
      submissionSparkConf: SparkConf,
      submissionSslConfiguration: DriverSubmitSslConfiguration): Unit = {
    val resolvedSparkConf = submissionSparkConf.clone()
    resolvedSparkConf.getOption("spark.app.id").foreach { id =>
      logWarning(s"Warning: Provided app id in spark.app.id as $id will be" +
        s" overridden as $kubernetesAppId")
    }
    resolvedSparkConf.set("spark.app.id", kubernetesAppId)
    resolvedSparkConf.setIfMissing("spark.app.name", kubernetesAppName)
    resolvedSparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
    resolvedSparkConf.setIfMissing("spark.blockmanager.port",
      DEFAULT_BLOCKMANAGER_PORT.toString)
    // Redact the OAuth token if present.
    resolvedSparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { _ =>
      resolvedSparkConf.set(KUBERNETES_SUBMIT_OAUTH_TOKEN, "<present_but_redacted>")
    }
    resolvedSparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).foreach { _ =>
      resolvedSparkConf.set(KUBERNETES_DRIVER_OAUTH_TOKEN, "<present_but_redacted>")
    }
    val driverSubmitter = HttpClientUtil.createClient[KubernetesSparkRestApi](
      uris = driverSubmissionServerUris,
      maxRetriesPerServer = 10,
      sslSocketFactory = submissionSslConfiguration
        .driverSubmitClientSslContext
        .getSocketFactory,
      trustContext = submissionSslConfiguration
        .driverSubmitClientTrustManager
        .orNull,
      connectTimeoutMillis = 5000)
    // Sanity check to see if the driver submitter is even reachable.
    driverSubmitter.ping()
    logInfo(s"Submitting local resources to driver pod for application " +
      s"$kubernetesAppId ...")
    val submitRequest = buildSubmissionRequest(resolvedSparkConf, submissionSecret)
    driverSubmitter.submitApplication(submitRequest)
    logInfo("Successfully submitted local resources and driver configuration to" +
      " driver pod.")
  }

  private def buildSubmissionRequest(
      sparkConf: SparkConf,
      submissionSecret: String): KubernetesCreateSubmissionRequest = {
    val mainResourceUri = Utils.resolveURI(mainAppResource)
    val resolvedAppResource: AppResource = Option(mainResourceUri.getScheme)
      .getOrElse("file") match {
      case "file" =>
        val appFile = new File(mainResourceUri.getPath)
        val fileBytes = Files.toByteArray(appFile)
        val fileBase64 = BaseEncoding.base64().encode(fileBytes)
        UploadedAppResource(resourceBase64Contents = fileBase64, name = appFile.getName)
      case "local" => ContainerAppResource(mainAppResource)
      case other => RemoteAppResource(other)
    }
    val uploadFilesBase64Contents = CompressionUtils.createTarGzip(submitterLocalFiles.map(
      Utils.resolveURI(_).getPath))
    val uploadJarsBase64Contents = CompressionUtils.createTarGzip(submitterLocalJars.map(
      Utils.resolveURI(_).getPath))
    KubernetesCreateSubmissionRequest(
      appResource = resolvedAppResource,
      mainClass = mainClass,
      appArgs = appArgs,
      secret = submissionSecret,
      sparkProperties = sparkConf.getAll.toMap,
      uploadedJarsBase64Contents = uploadJarsBase64Contents,
      uploadedFilesBase64Contents = uploadFilesBase64Contents,
      driverPodKubernetesCredentials = driverPodKubernetesCredentials)
  }
}
