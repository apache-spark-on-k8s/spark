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
package org.apache.spark.deploy.rest

import java.io.File

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64

import org.apache.spark.SPARK_VERSION
import org.apache.spark.util.Utils

case class KubernetesCreateSubmissionRequest(
  appResource: AppResource,
  mainClass: String,
  appArgs: Array[String],
  sparkProperties: Map[String, String],
  secret: String,
  uploadedJarsBase64Contents: Option[TarGzippedData],
  uploadedFilesBase64Contents: Option[TarGzippedData]) extends SubmitRestProtocolRequest {
  message = "create"
  clientSparkVersion = SPARK_VERSION
}

case class TarGzippedData(
  dataBase64: String,
  blockSize: Int = 10240,
  recordSize: Int = 512,
  encoding: String
)

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(value = Array(
  new JsonSubTypes.Type(value = classOf[UploadedAppResource], name = "UploadedAppResource"),
  new JsonSubTypes.Type(value = classOf[ContainerAppResource], name = "ContainerLocalAppResource"),
  new JsonSubTypes.Type(value = classOf[RemoteAppResource], name = "RemoteAppResource")))
abstract class AppResource

case class UploadedAppResource(
  resourceBase64Contents: String,
  name: String = "spark-app-resource") extends AppResource

case class ContainerAppResource(resourcePath: String) extends AppResource

case class RemoteAppResource(resource: String) extends AppResource

class PingResponse extends SubmitRestProtocolResponse {
  val text = "pong"
  message = "pong"
  serverSparkVersion = SPARK_VERSION
}

object AppResource {
  def assemble(appResource: String): AppResource = {
    val appResourceUri = Utils.resolveURI(appResource)
    appResourceUri.getScheme match {
      case "file" | null =>
        val appFile = new File(appResourceUri.getPath)
        if (!appFile.isFile) {
          throw new IllegalStateException("Provided local file path does not exist" +
            s" or is not a file: ${appFile.getAbsolutePath}")
        }
        val fileBytes = Files.toByteArray(appFile)
        val fileBase64 = Base64.encodeBase64String(fileBytes)
        UploadedAppResource(resourceBase64Contents = fileBase64, name = appFile.getName)
      case "container" | "local" => ContainerAppResource(appResourceUri.getPath)
      case other => RemoteAppResource(other)
    }
  }
}