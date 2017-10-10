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
package org.apache.spark.deploy.k8s.submit.submitsteps

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.submitsteps.hadoopsteps.{HadoopConfigSpec, HadoopConfigurationStep}


private[spark] class HadoopConfigBootstrapStepSuite extends SparkFunSuite with BeforeAndAfter{
  private val CONFIG_MAP_NAME = "config-map"
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val EXPECTED_SECRET = new SecretBuilder()
    .withNewMetadata()
    .withName(HADOOP_KERBEROS_SECRET_NAME)
    .endMetadata()
    .addToData("data", "secretata")
    .build()

  @Mock
  private var hadoopConfigStep : HadoopConfigurationStep = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopConfigStep.configureContainers(any[HadoopConfigSpec])).thenReturn(
      HadoopConfigSpec(
        configMapProperties = Map("data" -> "dataBytesToString"),
        driverPod = new PodBuilder()
          .withNewMetadata()
          .addToLabels("bootstrap", "true")
          .endMetadata()
          .withNewSpec().endSpec()
          .build(),
        driverContainer = new ContainerBuilder().withName(DRIVER_CONTAINER_NAME).build(),
        additionalDriverSparkConf = Map("sparkConf" -> "confValue"),
        dtSecret =
          Some(EXPECTED_SECRET),
        dtSecretName = HADOOP_KERBEROS_SECRET_NAME,
        dtSecretItemKey = ""))
  }

  test("Test modification of driverSpec with Hadoop Steps") {
    val hadoopConfStep = new HadoopConfigBootstrapStep(
      Seq(hadoopConfigStep),
      CONFIG_MAP_NAME)
    val expectedDriverSparkConf = new SparkConf(true)
      .set(HADOOP_CONFIG_MAP_SPARK_CONF_NAME, CONFIG_MAP_NAME)
      .set("sparkConf", "confValue")
    val expectedConfigMap = new ConfigMapBuilder()
      .withNewMetadata()
        .withName(CONFIG_MAP_NAME)
        .endMetadata()
      .addToData(Map("data" -> "dataBytesToString").asJava)
      .build()
    val expectedResources = Seq(expectedConfigMap, EXPECTED_SECRET)
    val driverSpec = KubernetesDriverSpec(
      driverPod = new Pod(),
      driverContainer = new Container(),
      driverSparkConf = new SparkConf(true),
      otherKubernetesResources = Seq.empty[HasMetadata])
    val returnContainerSpec = hadoopConfStep.configureDriver(driverSpec)
    assert(expectedDriverSparkConf.getAll === returnContainerSpec.driverSparkConf.getAll)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.otherKubernetesResources === expectedResources)
  }
}
