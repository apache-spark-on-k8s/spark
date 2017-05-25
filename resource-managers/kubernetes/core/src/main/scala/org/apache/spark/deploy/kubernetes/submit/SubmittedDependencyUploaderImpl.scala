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
package org.apache.spark.deploy.kubernetes.submit

import java.io.{File, FileOutputStream}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import okhttp3.RequestBody
import retrofit2.Call

import org.apache.spark.{SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.rest.kubernetes.{PodMonitoringCredentials, ResourceStagingServiceRetrofit, RetrofitClientFactory}
import org.apache.spark.util.Utils

private[spark] trait SubmittedDependencyUploader {
  /**
   * Upload submitter-local jars to the resource staging server.
   * @return The resource ID and secret to use to retrieve these jars.
   */
  def uploadJars(): SubmittedResourceIdAndSecret

  /**
   * Upload submitter-local files to the resource staging server.
   * @return The resource ID and secret to use to retrieve these files.
   */
  def uploadFiles(): SubmittedResourceIdAndSecret
}

/**
 * Default implementation of a SubmittedDependencyManager that is backed by a
 * Resource Staging Service.
 */
private[spark] class SubmittedDependencyUploaderImpl(
    kubernetesAppId: String,
    podLabels: Map[String, String],
    podNamespace: String,
    stagingServerUri: String,
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    clientKeyFile: Option[File],
    clientCertFile: Option[File],
    oauthToken: Option[String],
    stagingServiceSslOptions: SSLOptions,
    retrofitClientFactory: RetrofitClientFactory) extends SubmittedDependencyUploader {
  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)
  private val BASE_64 = BaseEncoding.base64()

  private def localUriStringsToFiles(uris: Seq[String]): Iterable[File] = {
    KubernetesFileUtils.getOnlySubmitterLocalFiles(uris)
      .map(Utils.resolveURI)
      .map(uri => new File(uri.getPath))
  }
  private def localJars: Iterable[File] = localUriStringsToFiles(sparkJars)
  private def localFiles: Iterable[File] = localUriStringsToFiles(sparkFiles)

  override def uploadJars(): SubmittedResourceIdAndSecret = doUpload(localJars, "uploaded-jars")
  override def uploadFiles(): SubmittedResourceIdAndSecret = doUpload(localFiles, "uploaded-files")

  private def doUpload(files: Iterable[File], fileNamePrefix: String)
      : SubmittedResourceIdAndSecret = {
    val filesDir = Utils.createTempDir(namePrefix = fileNamePrefix)
    val filesTgz = new File(filesDir, s"$fileNamePrefix.tgz")
    Utils.tryWithResource(new FileOutputStream(filesTgz)) { filesOutputStream =>
      CompressionUtils.writeTarGzipToStream(filesOutputStream, files.map(_.getAbsolutePath))
    }
    val clientKeyBase64 = clientKeyFile.map(f => BASE_64.encode(Files.toByteArray(f)))
    val clientCertBase64 = clientCertFile.map(f => BASE_64.encode(Files.toByteArray(f)))
    val oauthTokenBase64 = oauthToken.map(token => BASE_64.encode(token.getBytes(Charsets.UTF_8)))

    val kubernetesCredentialsString = OBJECT_MAPPER.writer()
      .writeValueAsString(PodMonitoringCredentials(
        clientKeyDataBase64 = clientKeyBase64,
        clientCertDataBase64 = clientCertBase64,
        oauthTokenBase64 = oauthTokenBase64))
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
