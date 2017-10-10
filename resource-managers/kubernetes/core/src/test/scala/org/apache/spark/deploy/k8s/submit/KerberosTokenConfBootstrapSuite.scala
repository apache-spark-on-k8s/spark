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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{KerberosTokenConfBootstrapImpl, PodWithMainContainer}
import org.apache.spark.deploy.k8s.constants._


private[spark] class KerberosTokenConfBootstrapSuite extends SparkFunSuite {
  private val SECRET_NAME = "dtSecret"
  private val SECRET_LABEL = "dtLabel"
  private val TEST_SPARK_USER = "hdfs"

  test("Test of bootstrapping kerberos secrets and env") {
    val kerberosConfStep = new KerberosTokenConfBootstrapImpl(
      SECRET_NAME,
      SECRET_LABEL,
      TEST_SPARK_USER)
    val expectedPod = new PodBuilder()
      .editOrNewSpec()
        .addNewVolume()
          .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
          .withNewSecret()
            .withSecretName(SECRET_NAME)
            .endSecret()
          .endVolume()
        .endSpec()
      .build()
    val podWithMain = PodWithMainContainer(
      new PodBuilder().withNewSpec().endSpec().build(),
      new Container())
    val returnedPodContainer = kerberosConfStep.bootstrapMainContainerAndVolumes(podWithMain)
    assert(expectedPod === returnedPodContainer.pod)
    assert(returnedPodContainer.mainContainer.getVolumeMounts.asScala.map(vm =>
      (vm.getName, vm.getMountPath)).head ===
      (SPARK_APP_HADOOP_SECRET_VOLUME_NAME, SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR))
    assert(returnedPodContainer.mainContainer.getEnv.asScala.head.getName ===
      ENV_HADOOP_TOKEN_FILE_LOCATION)
    assert(returnedPodContainer.mainContainer.getEnv.asScala(1).getName === ENV_SPARK_USER)
  }
}
