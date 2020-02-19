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
import java.util.UUID

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.apache.commons.io.FileUtils.readFileToString
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{HadoopConfBootstrap, PodWithMainContainer}
import org.apache.spark.deploy.k8s.constants.HADOOP_CONF_DIR_LOC
import org.apache.spark.util.Utils


private[spark] class HadoopConfMounterStepSuite extends SparkFunSuite with BeforeAndAfter{
  private val CONFIG_MAP_NAME = "config-map"
  private val HADOOP_CONF_DIR_VAL = "/etc/hadoop"
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val TEMP_HADOOP_FILE = createTempFile("core-site.xml")
  private val HADOOP_FILES = Seq(TEMP_HADOOP_FILE)

  @Mock
  private var hadoopConfBootstrap : HadoopConfBootstrap = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopConfBootstrap.bootstrapMainContainerAndVolumes(
      any[PodWithMainContainer])).thenAnswer(new Answer[PodWithMainContainer] {
      override def answer(invocation: InvocationOnMock) : PodWithMainContainer = {
        val pod = invocation.getArgumentAt(0, classOf[PodWithMainContainer])
        pod.copy(
          pod =
            new PodBuilder(pod.pod)
              .withNewMetadata()
                .addToLabels("bootstrap", "true")
                .endMetadata()
                .withNewSpec().endSpec()
              .build(),
          mainContainer =
            new ContainerBuilder()
              .withName(DRIVER_CONTAINER_NAME).build()
        )}})
  }

  test("Test of mounting hadoop_conf_dir files into HadoopConfigSpec") {
    val hadoopConfStep = new HadoopConfMounterStep(
      CONFIG_MAP_NAME,
      HADOOP_FILES,
      hadoopConfBootstrap,
      Some(HADOOP_CONF_DIR_VAL))
    val expectedDriverSparkConf = Map(HADOOP_CONF_DIR_LOC -> HADOOP_CONF_DIR_VAL)
    val expectedConfigMap = Map(
      TEMP_HADOOP_FILE.toPath.getFileName.toString ->
        readFileToString(TEMP_HADOOP_FILE))
    val hadoopConfSpec = HadoopConfigSpec(
      Map.empty[String, String],
      new Pod(),
      new Container(),
      Map.empty[String, String],
      None,
      "",
      "")
    val returnContainerSpec = hadoopConfStep.configureContainers(hadoopConfSpec)
    assert(expectedDriverSparkConf === returnContainerSpec.additionalDriverSparkConf)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.configMapProperties === expectedConfigMap)
  }

  private def createTempFile(contents: String): File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(contents.getBytes, file)
    file
  }
}
