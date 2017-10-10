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
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.HadoopUGIUtil
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.util.Utils



private[spark] class HadoopKerberosKeytabResolverStepSuite
  extends SparkFunSuite with BeforeAndAfter{
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val TEMP_KEYTAB_FILE = createTempFile("keytab")
  private val KERB_PRINCIPAL = "user@k8s.com"
  private val SPARK_USER_VALUE = "sparkUser"
  private val TEST_TOKEN_VALUE = "data"
  private def getByteArray(input: String) = input.toCharArray.map(_.toByte)
  private val TEST_DATA = getByteArray(TEST_TOKEN_VALUE)
  private val OUTPUT_TEST_DATA = Base64.encodeBase64String(TEST_DATA)
  private val INTERVAL = 500L
  private val CURR_TIME = System.currentTimeMillis()
  private val DATA_KEY_NAME =
    s"$KERBEROS_SECRET_LABEL_PREFIX-$CURR_TIME-$INTERVAL"
  private val SECRET_NAME = s"$HADOOP_KERBEROS_SECRET_NAME.$CURR_TIME"

  private val hadoopUGI = new HadoopUGIUtil()

  @Mock
  private var hadoopUtil: HadoopUGIUtil = _

  @Mock
  private var ugi: UserGroupInformation = _

  @Mock
  private var creds: Credentials = _

  @Mock
  private var token: Token[AbstractDelegationTokenIdentifier] = _

  @Mock
  private var identifier: AbstractDelegationTokenIdentifier = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopUtil.loginUserFromKeytabAndReturnUGI(any[String], any[String]))
      .thenAnswer(new Answer[UserGroupInformation] {
      override def answer(invocation: InvocationOnMock): UserGroupInformation = {
        hadoopUGI.getCurrentUser
      }
    })
    when(hadoopUtil.getCurrentUser).thenReturn(ugi)
    when(hadoopUtil.getShortName).thenReturn(SPARK_USER_VALUE)
    when(hadoopUtil.dfsAddDelegationToken(any(), any(), any())).thenReturn(null)
    when(ugi.getCredentials).thenReturn(creds)
    val tokens = List[Token[_ <: TokenIdentifier]](token).asJavaCollection
    when(creds.getAllTokens).thenReturn(tokens)
    when(hadoopUtil.serialize(any[Credentials]))
      .thenReturn(TEST_DATA)
    when(token.decodeIdentifier()).thenReturn(identifier)
    when(hadoopUtil.getCurrentTime).thenReturn(CURR_TIME)
    when(hadoopUtil.getTokenRenewalInterval(any[Iterable[Token[_ <: TokenIdentifier]]],
      any[Configuration])).thenReturn(Some(INTERVAL))
  }

  test("Testing keytab login") {
    when(hadoopUtil.isSecurityEnabled).thenReturn(true)
    val keytabStep = new HadoopKerberosKeytabResolverStep(
      new SparkConf(),
      Some(KERB_PRINCIPAL),
      Some(TEMP_KEYTAB_FILE),
      None,
      hadoopUtil)
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
    assert(returnContainerSpec.additionalDriverSparkConf(HADOOP_KERBEROS_CONF_ITEM_KEY)
        .contains(KERBEROS_SECRET_LABEL_PREFIX))
    assert(returnContainerSpec.additionalDriverSparkConf ===
      Map(HADOOP_KERBEROS_CONF_ITEM_KEY -> DATA_KEY_NAME,
        HADOOP_KERBEROS_CONF_SECRET -> SECRET_NAME))
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.dtSecretItemKey === DATA_KEY_NAME)
    assert(returnContainerSpec.dtSecret.get.getData.asScala === Map(
      DATA_KEY_NAME -> OUTPUT_TEST_DATA))
    assert(returnContainerSpec.dtSecretName === SECRET_NAME)
    assert(returnContainerSpec.dtSecret.get.getMetadata.getLabels.asScala ===
      Map("refresh-hadoop-tokens" -> "yes"))
    assert(returnContainerSpec.dtSecret.nonEmpty)
    assert(returnContainerSpec.dtSecret.get.getMetadata.getName === SECRET_NAME)
  }

  private def createTempFile(contents: String): File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(contents.getBytes, file)
    file
  }
}
