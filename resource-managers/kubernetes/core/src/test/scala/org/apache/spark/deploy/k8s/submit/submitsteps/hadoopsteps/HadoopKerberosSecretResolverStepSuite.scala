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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.constants._

private[spark] class HadoopKerberosSecretResolverStepSuite extends SparkFunSuite {
  private val CONFIG_MAP_NAME = "config-map"
  private val HADOOP_CONF_DIR_VAL = "/etc/hadoop"
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val TOKEN_SECRET_NAME = "secretName"
  private val TOKEN_SECRET_DATA_ITEM_KEY = "secretItemKey"

  test("Testing kerberos with Secret") {
    val keytabStep = new HadoopKerberosSecretResolverStep(
      new SparkConf(),
      TOKEN_SECRET_NAME,
      TOKEN_SECRET_DATA_ITEM_KEY)
    val expectedDriverSparkConf = Map(
      HADOOP_KERBEROS_CONF_ITEM_KEY -> TOKEN_SECRET_DATA_ITEM_KEY,
      HADOOP_KERBEROS_CONF_SECRET -> TOKEN_SECRET_NAME)
    val hadoopConfSpec = HadoopConfigSpec(
      Map.empty[String, String],
      new PodBuilder()
        .withNewMetadata()
        .addToLabels("bootstrap", "true")
        .endMetadata()
        .withNewSpec().endSpec()
        .build(),
      new ContainerBuilder().withName(DRIVER_CONTAINER_NAME).build(),
      Map.empty[String, String],
      None,
      "",
      "")
    val returnContainerSpec = keytabStep.configureContainers(hadoopConfSpec)
    assert(expectedDriverSparkConf === returnContainerSpec.additionalDriverSparkConf)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.dtSecret === None)
    assert(returnContainerSpec.dtSecretItemKey === TOKEN_SECRET_DATA_ITEM_KEY)
    assert(returnContainerSpec.dtSecretName === TOKEN_SECRET_NAME)
  }
}
