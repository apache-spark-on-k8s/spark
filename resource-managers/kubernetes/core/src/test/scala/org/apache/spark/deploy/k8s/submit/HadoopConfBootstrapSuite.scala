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
package org.apache.spark.deploy.k8s.submit

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{HadoopConfBootstrapImpl, HadoopUGIUtil, PodWithMainContainer}
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.util.Utils

private[spark] class HadoopConfBootstrapSuite extends SparkFunSuite with BeforeAndAfter{
  private val CONFIG_MAP_NAME = "config-map"
  private val TEMP_HADOOP_FILE = createTempFile("core-site.xml")
  private val HADOOP_FILES = Seq(TEMP_HADOOP_FILE)
  private val SPARK_USER_VALUE = "sparkUser"

  @Mock
  private var hadoopUtil: HadoopUGIUtil = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopUtil.getShortName).thenReturn(SPARK_USER_VALUE)
  }

  test("Test of bootstrapping hadoop_conf_dir files") {
    val hadoopConfStep = new HadoopConfBootstrapImpl(
      CONFIG_MAP_NAME,
      HADOOP_FILES,
      hadoopUtil)
    val expectedKeyPaths = Seq(
      new KeyToPathBuilder()
        .withKey(TEMP_HADOOP_FILE.toPath.getFileName.toString)
        .withPath(TEMP_HADOOP_FILE.toPath.getFileName.toString)
        .build())
    val expectedPod = new PodBuilder()
      .editOrNewSpec()
        .addNewVolume()
          .withName(HADOOP_FILE_VOLUME)
            .withNewConfigMap()
              .withName(CONFIG_MAP_NAME)
              .withItems(expectedKeyPaths.asJava)
              .endConfigMap()
          .endVolume()
        .endSpec()
      .build()

    val podWithMain = PodWithMainContainer(
      new PodBuilder().withNewSpec().endSpec().build(),
      new Container())
    val returnedPodContainer = hadoopConfStep.bootstrapMainContainerAndVolumes(podWithMain)
    assert(expectedPod === returnedPodContainer.pod)
    assert(returnedPodContainer.mainContainer.getVolumeMounts.asScala.map(vm =>
      (vm.getName, vm.getMountPath)).head === (HADOOP_FILE_VOLUME, HADOOP_CONF_DIR_PATH))
    assert(returnedPodContainer.mainContainer.getEnv.asScala.head ===
    new EnvVarBuilder().withName(ENV_HADOOP_CONF_DIR).withValue(HADOOP_CONF_DIR_PATH).build())
    assert(returnedPodContainer.mainContainer.getEnv.asScala(1) ===
      new EnvVarBuilder().withName(ENV_SPARK_USER).withValue(SPARK_USER_VALUE).build())
  }
  private def createTempFile(contents: String): File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(contents.getBytes, file)
    file
  }
}
