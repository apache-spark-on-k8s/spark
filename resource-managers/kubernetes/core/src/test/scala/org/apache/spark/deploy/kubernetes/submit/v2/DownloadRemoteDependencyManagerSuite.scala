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

import java.io.StringReader
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, Container, PodBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

class DownloadRemoteDependencyManagerSuite extends SparkFunSuite {

  private val APP_ID = "app-id"
  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/jar1.jar",
    "local:///app/jars/jar2.jar",
    "http://localhost:8080/jar2.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/file.txt",
    "file:///app/files/file.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/spark-data/spark-jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/spark-files/spark-files"
  private val INIT_CONTAINER_IMAGE = "spark-driver-init:latest"
  private val dependencyManagerUnderTest = new DownloadRemoteDependencyManagerImpl(
    APP_ID,
    SPARK_JARS,
    SPARK_FILES,
    JARS_DOWNLOAD_PATH,
    FILES_DOWNLOAD_PATH,
    INIT_CONTAINER_IMAGE)

  test("Config map should set the files to download") {
    val configMap = dependencyManagerUnderTest.buildInitContainerConfigMap()
    assert(configMap.getMetadata.getName === s"$APP_ID-remote-files-download-init")
    val configProperties = configMap.getData.asScala
    assert(configProperties.size === 1)
    val propertiesString = configProperties(INIT_CONTAINER_REMOTE_FILES_CONFIG_MAP_KEY)
    assert(propertiesString != null)
    val propertiesReader = new StringReader(propertiesString)
    val initContainerProperties = new Properties()
    initContainerProperties.load(propertiesReader)
    assert(initContainerProperties.size() === 4)
    val downloadRemoteJars = initContainerProperties.getProperty(INIT_CONTAINER_REMOTE_JARS.key)
    assert(downloadRemoteJars != null)
    val downloadRemoteJarsSplit = downloadRemoteJars.split(",").toSet
    val expectedRemoteJars = Set(
      "hdfs://localhost:9000/jar1.jar", "http://localhost:8080/jar2.jar")
    assert(expectedRemoteJars === downloadRemoteJarsSplit)
    val downloadRemoteFiles = initContainerProperties.getProperty(INIT_CONTAINER_REMOTE_FILES.key)
    assert(downloadRemoteFiles != null)
    val downloadRemoteFilesSplit = downloadRemoteFiles.split(",").toSet
    val expectedRemoteFiles = Set("hdfs://localhost:9000/file.txt")
    assert(downloadRemoteFilesSplit === expectedRemoteFiles)
    assert(initContainerProperties.getProperty(REMOTE_JARS_DOWNLOAD_LOCATION.key) ===
      JARS_DOWNLOAD_PATH)
    assert(initContainerProperties.getProperty(REMOTE_FILES_DOWNLOAD_LOCATION.key) ===
      FILES_DOWNLOAD_PATH)
  }

  test("Resolving the local classpath should map remote jars to their downloaded locations") {
    val resolvedLocalClasspath = dependencyManagerUnderTest.resolveLocalClasspath()
    val expectedLocalClasspath = Set(
      s"$JARS_DOWNLOAD_PATH/jar1.jar",
      s"$JARS_DOWNLOAD_PATH/jar2.jar",
      "/app/jars/jar2.jar")
    assert(resolvedLocalClasspath.toSet === expectedLocalClasspath)
  }
}
