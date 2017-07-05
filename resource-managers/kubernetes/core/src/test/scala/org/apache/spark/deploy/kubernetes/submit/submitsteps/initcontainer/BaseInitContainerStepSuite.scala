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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.initcontainer

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, Pod}
import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.{PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfter}

class BaseInitContainerStepSuite extends SparkFunSuite with BeforeAndAfter{
  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar", "file:///app/jars/jar2.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt", "file:///app/files/file2.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/files"
  private val CONFIG_MAP_NAME = "config-map"
  private val CONFIG_MAP_KEY = "config-map-key"

  @Mock
  private var podAndInitContainerBootstrap : SparkPodInitContainerBootstrap = _
  @Mock
  private var podWithDetachedInitContainer : PodWithDetachedInitContainer = _

  before {
    MockitoAnnotations.initMocks(this)
    when(podAndInitContainerBootstrap.bootstrapInitContainerAndVolumes(
      any[PodWithDetachedInitContainer])).thenReturn(podWithDetachedInitContainer)
  }

  test("Test of additionalDriverSparkConf with mix of remote files and jars") {
    val baseInitStep = new BaseInitContainerStep(
      SPARK_JARS,
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY,
      podAndInitContainerBootstrap)
    val remoteJarsToDownload = KubernetesFileUtils.getOnlyRemoteFiles(SPARK_JARS)
    val remoteFilesToDownload = KubernetesFileUtils.getOnlyRemoteFiles(SPARK_FILES)
    assert(remoteJarsToDownload === List("hdfs://localhost:9000/app/jars/jar1.jar"))
    assert(remoteFilesToDownload === List("hdfs://localhost:9000/app/files/file1.txt"))
    val expectedTest = Map(
      INIT_CONTAINER_JARS_DOWNLOAD_LOCATION.key -> JARS_DOWNLOAD_PATH,
      INIT_CONTAINER_FILES_DOWNLOAD_LOCATION.key -> FILES_DOWNLOAD_PATH,
      INIT_CONTAINER_REMOTE_JARS.key -> "hdfs://localhost:9000/app/jars/jar1.jar",
      INIT_CONTAINER_REMOTE_FILES.key -> "hdfs://localhost:9000/app/files/file1.txt"
    )
    val initContainerSpec = InitContainerSpec(
      Map.empty[String, String], Map.empty[String, String],
      new Container(), new Container(), new Pod, Seq.empty[HasMetadata]
    )
    val returnContainerSpec = baseInitStep.prepareInitContainer(initContainerSpec)
    assert(expectedTest.toSet.subsetOf(returnContainerSpec.initContainerProperties.toSet))
  }
}
