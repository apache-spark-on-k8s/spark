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
package org.apache.spark.deploy.kubernetes.integrationtest.kerberos

import java.io.{File, FileInputStream}

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.extensions.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.io.FileUtils.readFileToString

import org.apache.spark.deploy.kubernetes.submit.ContainerNameEqualityPredicate


private[spark] class KerberosUtils(
  kubernetesClient: KubernetesClient,
  namespace: String) {
  def getClient: KubernetesClient = kubernetesClient
  def getNamespace: String = namespace
  def yamlLocation(loc: String): String = s"kerberos-yml/$loc.yml"
  def loadFromYaml(resource: String): FileInputStream =
    new FileInputStream(new File(yamlLocation(resource)))
  private val regex = "REPLACE_ME".r
  private def locationResolver(loc: String) = s"src/test/resources/$loc"
  private val kerberosFiles = Seq("krb5.conf", "core-site.xml", "hdfs-site.xml")
  private val kerberosConfTupList =
    kerberosFiles.map { file =>
      (file, regex.replaceAllIn(readFileToString(new File(locationResolver(file))), namespace))}
  private val KRB_VOLUME = "krb5-conf"
  private val KRB_FILE_DIR = "/tmp"
  private val KRB_CONFIG_MAP_NAME = "krb-config-map"
  private val PV_LABELS = Map("job" -> "kerberostest")
  private val keyPaths: Seq[KeyToPath] = kerberosFiles.map(file =>
    new KeyToPathBuilder()
      .withKey(file)
      .withPath(file)
      .build()).toList
  private val pvNN = Seq("namenode-hadoop", "namenode-hadoop-pv")
  private val pvKT = Seq("server-keytab", "server-keytab-pv")
  private def buildKerberosPV(seqPair: Seq[String]) = {
    KerberosStorage(
      kubernetesClient.load(loadFromYaml(seqPair.head))
        .get().get(0).asInstanceOf[PersistentVolumeClaim],
      kubernetesClient.load(loadFromYaml(seqPair(1)))
        .get().get(0).asInstanceOf[PersistentVolume])
  }
  def getNNStorage: KerberosStorage = buildKerberosPV(pvNN)
  def getKTStorage: KerberosStorage = buildKerberosPV(pvKT)
  def getLabels: Map[String, String] = PV_LABELS
  def getPVNN: Seq[String] = pvNN
  def getKeyPaths: Seq[KeyToPath] = keyPaths
  def getConfigMap: ConfigMap = new ConfigMapBuilder()
    .withNewMetadata()
    .withName(KRB_CONFIG_MAP_NAME)
    .endMetadata()
    .addToData(kerberosConfTupList.toMap.asJava)
    .build()
  private val kdcNode = Seq("kerberos-deployment", "kerberos-service")
  private val nnNode = Seq("nn-deployment", "nn-service")
  private val dnNode = Seq("dn1-deployment", "dn1-service")
  private val dataPopulator = Seq("data-populator-deployment", "data-populator-service")
  private def buildKerberosDeployment(seqPair: Seq[String]) = {
    val deployment =
      kubernetesClient.load(loadFromYaml(seqPair.head)).get().get(0).asInstanceOf[Deployment]
    KerberosDeployment(
      new DeploymentBuilder(deployment)
        .editSpec()
          .editTemplate()
            .editSpec()
              .addNewVolume()
                .withName(KRB_VOLUME)
                .withNewConfigMap()
                  .withName(KRB_CONFIG_MAP_NAME)
                  .withItems(keyPaths.asJava)
                  .endConfigMap()
                .endVolume()
            .editMatchingContainer(new ContainerNameEqualityPredicate(
              deployment.getMetadata.getName))
              .addNewEnv()
                .withName("NAMESPACE")
                .withValue(namespace)
                .endEnv()
              .addNewEnv()
                .withName("TMP_KRB_LOC")
                .withValue(s"$KRB_FILE_DIR/${kerberosFiles.head}")
                .endEnv()
              .addNewEnv()
                .withName("TMP_CORE_LOC")
                .withValue(s"$KRB_FILE_DIR/${kerberosFiles(1)}")
                .endEnv()
              .addNewEnv()
                .withName("TMP_HDFS_LOC")
                .withValue(s"$KRB_FILE_DIR/${kerberosFiles(2)}")
                .endEnv()
              .addNewVolumeMount()
                .withName(KRB_VOLUME)
                .withMountPath(KRB_FILE_DIR)
                .endVolumeMount()
              .endContainer()
            .endSpec()
          .endTemplate()
        .endSpec()
      .build(),
      kubernetesClient.load(loadFromYaml(seqPair(1))).get().get(0).asInstanceOf[Service]
    )
  }
  def getKDC: KerberosDeployment = buildKerberosDeployment(kdcNode)
  def getNN: KerberosDeployment = buildKerberosDeployment(nnNode)
  def getDN: KerberosDeployment = buildKerberosDeployment(dnNode)
  def getDP: KerberosDeployment = buildKerberosDeployment(dataPopulator)
}
