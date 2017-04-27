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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.{File, FileOutputStream}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ConfigMap, Secret, SecretBuilder}
import okhttp3.RequestBody
import retrofit2.Call
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.apache.spark.deploy.kubernetes.KubernetesCredentials
import org.apache.spark.deploy.rest.kubernetes.v2.{ResourceStagingServiceRetrofit, RetrofitClientFactory, StagedResourceIdentifier}
import org.apache.spark.util.Utils

private[spark] trait SubmittedDependencyManager {

  /**
   * Upload submitter-local jars to the resource staging server.
   * @return The resource ID and secret to use to retrieve these jars.
   */
  def uploadJars(): StagedResourceIdentifier

  /**
   * Upload submitter-local files to the resource staging server.
   * @return The resource ID and secret to use to retrieve these files.
   */
  def uploadFiles(): StagedResourceIdentifier

  def getInitContainerBootstrap(
    initContainerSecret: Secret,
    initContainerConfigMap: ConfigMap): SparkPodInitContainerBootstrap

  def buildInitContainerSecret(jarsSecret: String, filesSecret: String): Secret

  def buildInitContainerConfigMap(
    jarsResourceId: String, filesResourceId: String): ConfigMap

  /**
   * Convert the Spark jar paths from their locations on the submitter's disk to
   * the locations they will be downloaded to on the driver's disk.
   */
  def resolveSparkJars(): Seq[String]

  /**
   * Convert the Spark file paths from their locations on the submitter's disk to
   * the locations they will be downloaded to on the driver's disk.
   */
  def resolveSparkFiles(): Seq[String]

  /**
   * Adjusts the Spark configuration such that the scheduler backend will configure
   * executor pods to attach an init-container that fetches these submitted dependencies.
   */
  def configureExecutorsToFetchSubmittedDependencies(
    sparkConf: SparkConf,
    initContainerConfigMap: ConfigMap,
    initContainerSecret: Secret): SparkConf
}

/**
 * Default implementation of a SubmittedDependencyManager that is backed by a
 * Resource Staging Service.
 */
private[spark] class SubmittedDependencyManagerImpl(
    kubernetesAppId: String,
    podLabels: Map[String, String],
    podNamespace: String,
    stagingServerUri: String,
    initContainerImage: String,
    jarsDownloadPath: String,
    filesDownloadPath: String,
    downloadTimeoutMinutes: Long,
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    stagingServiceSslOptions: SSLOptions,
    retrofitClientFactory: RetrofitClientFactory) extends SubmittedDependencyManager {
  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)

  private def localUriStringsToFiles(uris: Seq[String]): Iterable[File] = {
    KubernetesFileUtils.getOnlySubmitterLocalFiles(uris)
      .map(Utils.resolveURI)
      .map(uri => new File(uri.getPath))
  }
  private def localJars: Iterable[File] = localUriStringsToFiles(sparkJars)
  private def localFiles: Iterable[File] = localUriStringsToFiles(sparkFiles)

  override def uploadJars(): StagedResourceIdentifier = doUpload(localJars, "uploaded-jars")
  override def uploadFiles(): StagedResourceIdentifier = doUpload(localFiles, "uploaded-files")

  private def doUpload(files: Iterable[File], fileNamePrefix: String): StagedResourceIdentifier = {
    val filesDir = Utils.createTempDir(namePrefix = fileNamePrefix)
    val filesTgz = new File(filesDir, s"$fileNamePrefix.tgz")
    Utils.tryWithResource(new FileOutputStream(filesTgz)) { filesOutputStream =>
      CompressionUtils.writeTarGzipToStream(filesOutputStream, files.map(_.getAbsolutePath))
    }
    // TODO provide credentials properly when the staging server monitors the Kubernetes API.
    val kubernetesCredentialsString = OBJECT_MAPPER.writer()
      .writeValueAsString(KubernetesCredentials(None, None, None, None))
    val labelsAsString = OBJECT_MAPPER.writer().writeValueAsString(podLabels)

    val filesRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.MULTIPART_FORM_DATA), filesTgz)

    val kubernetesCredentialsBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), kubernetesCredentialsString)

    val namespaceRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.TEXT_PLAIN), podNamespace)

    val labelsRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), labelsAsString)

    val service = retrofitClientFactory.createRetrofitClient(
      stagingServerUri,
      classOf[ResourceStagingServiceRetrofit],
      stagingServiceSslOptions)
    val uploadResponse = service.uploadResources(
      labelsRequestBody, namespaceRequestBody, filesRequestBody, kubernetesCredentialsBody)
    getTypedResponseResult(uploadResponse)
  }

  override def getInitContainerBootstrap(
      initContainerSecret: Secret,
      initContainerConfigMap: ConfigMap): SparkPodInitContainerBootstrap = {
    new SparkPodInitContainerBootstrapImpl(
      INIT_CONTAINER_SUBMITTED_FILES_SUFFIX,
      initContainerConfigMap.getMetadata.getName,
      INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY,
      initContainerImage,
      jarsDownloadPath,
      filesDownloadPath,
      Some(InitContainerSecretConfiguration(
        secretName = initContainerSecret.getMetadata.getName,
        secretMountPath = INIT_CONTAINER_SUBMITTED_FILES_SECRETS_VOLUME_MOUNT_PATH)))
  }

  override def buildInitContainerSecret(jarsSecret: String, filesSecret: String): Secret = {
    val trustStoreBase64 = stagingServiceSslOptions.trustStore.map { trustStoreFile =>
      require(trustStoreFile.isFile, "Dependency server trustStore provided at" +
        trustStoreFile.getAbsolutePath + " does not exist or is not a file.")
      (INIT_CONTAINER_SUBMITTED_FILES_TRUSTSTORE_SECRET_KEY,
        BaseEncoding.base64().encode(Files.toByteArray(trustStoreFile)))
    }.toMap
    val jarsSecretBase64 = BaseEncoding.base64().encode(jarsSecret.getBytes(Charsets.UTF_8))
    val filesSecretBase64 = BaseEncoding.base64().encode(filesSecret.getBytes(Charsets.UTF_8))
    val secretData = Map(
      INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_JARS_SECRET_KEY -> jarsSecretBase64,
      INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_FILES_SECRET_KEY -> filesSecretBase64) ++
      trustStoreBase64
    val kubernetesSecret = new SecretBuilder()
      .withNewMetadata()
      .withName(s"$kubernetesAppId-spark-init")
      .endMetadata()
      .addToData(secretData.asJava)
      .build()
    kubernetesSecret
  }

  override def buildInitContainerConfigMap(
       jarsResourceId: String, filesResourceId: String): ConfigMap = {
    val initContainerConfig = Map[String, String](
      RESOURCE_STAGING_SERVER_URI.key -> stagingServerUri,
      SUBMITTED_JARS_DOWNLOAD_LOCATION.key -> jarsDownloadPath,
      SUBMITTED_FILES_DOWNLOAD_LOCATION.key -> filesDownloadPath,
      INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER.key -> jarsResourceId,
      INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION.key ->
        INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_JARS_SECRET_PATH,
      INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER.key -> filesResourceId,
      INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION.key ->
        INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_FILES_SECRET_PATH,
      MOUNT_DEPENDENCIES_INIT_TIMEOUT.key -> s"${downloadTimeoutMinutes}m",
      RESOURCE_STAGING_SERVER_SSL_ENABLED.key -> stagingServiceSslOptions.enabled.toString) ++
      stagingServiceSslOptions.trustStore.map { _ =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE.key,
          INIT_CONTAINER_SUBMITTED_FILES_TRUSTSTORE_PATH)
      }.toMap ++
      stagingServiceSslOptions.trustStorePassword.map { password =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD.key, password)
      }.toMap ++
      stagingServiceSslOptions.trustStoreType.map { storeType =>
        (RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE.key, storeType)
      }.toMap
    PropertiesConfigMapFromScalaMapBuilder.buildConfigMap(
      s"$kubernetesAppId-staging-server-download-init",
      INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY,
      initContainerConfig)
  }

  override def resolveSparkJars(): Seq[String] = resolveLocalFiles(sparkJars, jarsDownloadPath)

  override def resolveSparkFiles(): Seq[String] = resolveLocalFiles(sparkFiles, filesDownloadPath)

  override def configureExecutorsToFetchSubmittedDependencies(
      sparkConf: SparkConf,
      initContainerConfigMap: ConfigMap,
      initContainerSecret: Secret): SparkConf = {
    sparkConf.clone()
      .set(EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP,
        initContainerConfigMap.getMetadata.getName)
      .set(EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY,
        INIT_CONTAINER_SUBMITTED_FILES_CONFIG_MAP_KEY)
      .set(EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET,
        initContainerSecret.getMetadata.getName)
      .set(EXECUTOR_INIT_CONTAINER_SUBMITTED_FILES_RESOURCE_STAGING_SERVER_SECRET_DIR,
        INIT_CONTAINER_SUBMITTED_FILES_SECRETS_VOLUME_MOUNT_PATH)
  }

  private def resolveLocalFiles(
      allFileUriStrings: Seq[String], localDownloadRoot: String): Seq[String] = {
    val usedLocalFileNames = mutable.HashSet.empty[String]
    val resolvedFiles = mutable.Buffer.empty[String]
    for (fileUriString <- allFileUriStrings) {
      val fileUri = Utils.resolveURI(fileUriString)
      val resolvedFile = Option(fileUri.getScheme).getOrElse("file") match {
        case "file" =>
          // Deduplication logic matches that of CompressionUtils#writeTarGzipToStream
          val file = new File(fileUri.getPath)
          val extension = Files.getFileExtension(file.getName)
          val nameWithoutExtension = Files.getNameWithoutExtension(file.getName)
          var resolvedFileName = file.getName
          var deduplicationCounter = 1
          while (usedLocalFileNames.contains(resolvedFileName)) {
            resolvedFileName = s"$nameWithoutExtension-$deduplicationCounter.$extension"
            deduplicationCounter += 1
          }
          s"file://$localDownloadRoot/$resolvedFileName"
        case _ => fileUriString
      }
      resolvedFiles += resolvedFile
    }
    resolvedFiles
  }

  private def getTypedResponseResult[T](call: Call[T]): T = {
    val response = call.execute()
    if (response.code() < 200 || response.code() >= 300) {
      throw new SparkException("Unexpected response from dependency server when uploading" +
        s" dependencies: ${response.code()}. Error body: " +
        Option(response.errorBody()).map(_.string()).getOrElse("N/A"))
    }
    response.body()
  }
}
