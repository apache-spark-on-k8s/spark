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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.config._

private[spark] class HadoopStepsOrchestratorSuite extends SparkFunSuite {
  private val NAMESPACE = "testNamespace"
  private val HADOOP_CONFIG_MAP = "hadoop-config-map"
  private val HADOOP_CONF_DIR_VAL = "/etc/hadoop/conf"
  private val KUBE_PREFIX = "spark-test"

  test("Testing without Kerberos") {
    val sparkTestConf = new SparkConf(true)
      .set(KUBERNETES_KERBEROS_SUPPORT, false)
    val hadoopOrchestrator = new HadoopStepsOrchestrator(
      KUBE_PREFIX,
      NAMESPACE,
      HADOOP_CONFIG_MAP,
      sparkTestConf,
      Some(HADOOP_CONF_DIR_VAL))
    val steps = hadoopOrchestrator.getHadoopSteps()
    assert(steps.length === 2)
    assert(steps.head.isInstanceOf[HadoopConfMounterStep])
    assert(steps(1).isInstanceOf[HadoopConfSparkUserStep])
  }

  test("Testing with Keytab Kerberos Login") {
    val sparkTestConf = new SparkConf(true)
      .set(KUBERNETES_KERBEROS_SUPPORT, true)
      .set(KUBERNETES_KERBEROS_KEYTAB, "keytab.file")
      .set(KUBERNETES_KERBEROS_PRINCIPAL, "user@kerberos")
    val hadoopOrchestrator = new HadoopStepsOrchestrator(
      KUBE_PREFIX,
      NAMESPACE,
      HADOOP_CONFIG_MAP,
      sparkTestConf,
      Some(HADOOP_CONF_DIR_VAL))
    val steps = hadoopOrchestrator.getHadoopSteps()
    assert(steps.length === 2)
    assert(steps.head.isInstanceOf[HadoopConfMounterStep])
    assert(steps(1).isInstanceOf[HadoopKerberosKeytabResolverStep])
  }

  test("Testing with kinit Kerberos Login") {
    val sparkTestConf = new SparkConf(true)
      .set(KUBERNETES_KERBEROS_SUPPORT, true)
    val hadoopOrchestrator = new HadoopStepsOrchestrator(
      KUBE_PREFIX,
      NAMESPACE,
      HADOOP_CONFIG_MAP,
      sparkTestConf,
      Some(HADOOP_CONF_DIR_VAL))
    val steps = hadoopOrchestrator.getHadoopSteps()
    assert(steps.length === 2)
    assert(steps.head.isInstanceOf[HadoopConfMounterStep])
    assert(steps(1).isInstanceOf[HadoopKerberosKeytabResolverStep])
  }

  test("Testing with Secret stored Kerberos") {
    val sparkTestConf = new SparkConf(true)
      .set(KUBERNETES_KERBEROS_SUPPORT, true)
      .set(KUBERNETES_KERBEROS_DT_SECRET_NAME, "dtSecret")
      .set(KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY, "dtItemKey")
    val hadoopOrchestrator = new HadoopStepsOrchestrator(
      KUBE_PREFIX,
      NAMESPACE,
      HADOOP_CONFIG_MAP,
      sparkTestConf,
      Some(HADOOP_CONF_DIR_VAL))
    val steps = hadoopOrchestrator.getHadoopSteps()
    assert(steps.length === 2)
    assert(steps.head.isInstanceOf[HadoopConfMounterStep])
    assert(steps(1).isInstanceOf[HadoopKerberosSecretResolverStep])
  }
}
