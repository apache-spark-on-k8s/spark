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
package org.apache.spark.deploy.kubernetes

import java.io.FileInputStream
import java.nio.file.Files
import java.security.KeyStore
import javax.net.ssl.SSLContext

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files => GuavaFiles}
import io.fabric8.kubernetes.api.model.{DoneableSecret, EnvVar, EnvVarBuilder, Secret, SecretBuilder, SecretList, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, Resource}
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar.{mock => simpleMock}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.util.Utils

private[spark] class SslConfigurationProviderSuite extends SparkFunSuite with BeforeAndAfter {

  private type SecretResource = Resource[Secret, DoneableSecret]
  private type SecretsHandler = MixedOperation[Secret, SecretList, DoneableSecret, SecretResource]

  private val APP_ID = "app-id"
  private val KEYSTORE_PASSWORD = "keystore"
  private val KEY_PASSWORD = "key"
  private val TRUSTSTORE_PASSWORD = "truststore"
  private val IP_ADDRESS = "192.168.99.100"
  private val SSL_SECRET_DIR = s"$DRIVER_CONTAINER_SECRETS_BASE_DIR/$APP_ID-ssl"

  private val sslFolder = Files.createTempDirectory("ssl-configuration-provider-suite").toFile
  sslFolder.deleteOnExit()
  private val (keyStore, trustStore) = SSLUtils.generateKeyStoreTrustStorePair(
    IP_ADDRESS, KEYSTORE_PASSWORD, KEY_PASSWORD, TRUSTSTORE_PASSWORD)

  private var sparkConf: SparkConf = _
  private var kubernetesClient: KubernetesClient = _
  private var kubernetesResourceCleaner: KubernetesResourceCleaner = _
  private var secretsHandler: SecretsHandler = _
  private var sslConfigurationProvider: SslConfigurationProvider = _

  before {
    sparkConf = new SparkConf(false)
    kubernetesClient = simpleMock[KubernetesClient]
    kubernetesResourceCleaner = simpleMock[KubernetesResourceCleaner]
    secretsHandler = simpleMock[SecretsHandler]
    sslConfigurationProvider = new SslConfigurationProvider(sparkConf, APP_ID, kubernetesClient,
      kubernetesResourceCleaner)
    when(kubernetesClient.secrets()).thenReturn(secretsHandler)
    when(secretsHandler.create(any())).thenAnswer(new Answer[Secret] {
      override def answer(invocationOnMock: InvocationOnMock): Secret = {
        invocationOnMock.getArgumentAt(0, classOf[Secret])
      }
    })
  }

  test("Disabling SSL should return empty components") {
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, false)
    val configuration = sslConfigurationProvider.getSslConfiguration()
    assert(!configuration.sslOptions.enabled, "SSL should not be enabled.")
    assert(configuration.driverSubmitClientSslContext == SSLContext.getDefault,
      "Should have returned the default SSL context.")
    assert(configuration.driverSubmitClientTrustManager.isEmpty, "Trust manager should be absent.")
    assert(configuration.sslPodEnvVars.isEmpty, "No environment variables should be defined.")
    assert(configuration.sslPodVolumes.isEmpty, "No SSL volumes should be defined.")
    assert(configuration.sslPodVolumeMounts.isEmpty, "No SSL volume mounts should be defined.")
    assert(configuration.sslSecrets.isEmpty, "No SSL secrets should be defined.")
  }

  test("Enabling SSL should load a keyStore and trustStore when provided.") {
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, true)
    sparkConf.set(KUBERNETES_DRIVER_SUBMIT_KEYSTORE, s"file://${keyStore.getAbsolutePath}")
    sparkConf.set(KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE, s"file://${trustStore.getAbsolutePath}")
    sparkConf.set("spark.ssl.kubernetes.submit.keyStorePassword", KEYSTORE_PASSWORD)
    sparkConf.set("spark.ssl.kubernetes.submit.keyPassword", KEY_PASSWORD)
    sparkConf.set("spark.ssl.kubernetes.submit.trustStorePassword", TRUSTSTORE_PASSWORD)
    sparkConf.set("spark.ssl.kubernetes.submit.keyStoreType", "jks")
    val configuration = sslConfigurationProvider.getSslConfiguration()
    assert(configuration.sslOptions.enabled, "SSL should be enabled.")
    assert(configuration.isKeyStoreLocalFile, "KeyStore should be treated as a local file.")
    val maybeReturnedKeyStore = configuration.sslOptions.keyStore
    assert(maybeReturnedKeyStore.isDefined, "KeyStore should be in the SSL Options.")
    maybeReturnedKeyStore.foreach { returnedKeyStore =>
      assert(returnedKeyStore.getAbsolutePath === keyStore.getAbsolutePath,
        "KeyStore paths did not match.")
    }
    val maybeReturnedTrustStore = configuration.sslOptions.trustStore
    assert(maybeReturnedTrustStore.isDefined, "TrustStore should be in the SSL Options.")
    maybeReturnedTrustStore.foreach { returnedTrustStore =>
      assert(returnedTrustStore.getAbsolutePath === trustStore.getAbsolutePath)
    }
    assertResult(configuration.sslPodEnvVars.toSet,
        "Environment vars for SSL did not match,") {
      Set[EnvVar](
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_FILE)
          .withValue(s"$SSL_SECRET_DIR/$SUBMISSION_SSL_KEYSTORE_SECRET_NAME")
          .build(),
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_PASSWORD_FILE)
          .withValue(s"$SSL_SECRET_DIR/$SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME")
          .build(),
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE)
          .withValue(s"$SSL_SECRET_DIR/$SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME")
          .build(),
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_TYPE)
          .withValue("jks")
          .build(),
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_USE_SSL)
          .withValue("true")
          .build())
    }
    val keyStoreBase64 = BaseEncoding.base64().encode(GuavaFiles.toByteArray(keyStore))
    val keyPasswordBase64 = BaseEncoding.base64().encode(KEY_PASSWORD.getBytes(Charsets.UTF_8))
    val keyStorePasswordBase64 = BaseEncoding
      .base64()
      .encode(KEYSTORE_PASSWORD.getBytes(Charsets.UTF_8))
    val expectedSecret = new SecretBuilder()
        .withNewMetadata()
        .withName(s"$SUBMISSION_SSL_SECRETS_PREFIX-$APP_ID")
        .endMetadata()
        .addToData(
          SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME,
          keyPasswordBase64)
        .addToData(
          SUBMISSION_SSL_KEYSTORE_SECRET_NAME,
          keyStoreBase64)
        .addToData(
          SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME,
          keyStorePasswordBase64)
        .withType("Opaque")
        .build()
    assert(configuration.sslSecrets.toSeq === Seq(expectedSecret),
      "SSL secret did not match.")
    assertResult(configuration.sslPodVolumes.toSeq, "SSL Volumes are incorrect.") {
      Seq[Volume](
        new VolumeBuilder()
          .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
          .withNewSecret()
            .withSecretName(s"$SUBMISSION_SSL_SECRETS_PREFIX-$APP_ID")
            .endSecret()
          .build()
      )
    }
    assertResult(configuration.sslPodVolumeMounts.toSeq, "SSL volume mounts are incorrect.") {
      Seq[VolumeMount](
        new VolumeMountBuilder()
          .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
          .withReadOnly(true)
          .withMountPath(SSL_SECRET_DIR)
          .build()
      )
    }
    assert(configuration.driverSubmitClientTrustManager.isDefined, "Missing trust manager.")
    configuration.driverSubmitClientTrustManager.foreach { trustManager =>
      val acceptedIssuers = trustManager.getAcceptedIssuers
      val inMemoryKeyStore = KeyStore.getInstance("jks")
      Utils.tryWithResource(new FileInputStream(keyStore)) { keyStoreStream =>
        inMemoryKeyStore.load(keyStoreStream, KEYSTORE_PASSWORD.toCharArray)
        val certChain = inMemoryKeyStore.getCertificateChain("key")
        val acceptedIssuersBytes = acceptedIssuers.map(_.getEncoded.toSeq)
        val certChainEncoded = certChain.map(_.getEncoded.toSeq)
        assert(acceptedIssuersBytes.toSeq === certChainEncoded.toSeq,
          "Certificates did not match.")
      }
    }
    assert(configuration.driverSubmitClientSslContext.getProtocol === "TLSv1.2",
      "SSL context protocol is incorrect.")
    verify(kubernetesClient).secrets()
    verify(secretsHandler).create(expectedSecret)
  }

  test("Providing a KeyStore with a local scheme should not mount it in a secret") {
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, true)
    sparkConf.set(KUBERNETES_DRIVER_SUBMIT_KEYSTORE, s"local:///opt/spark/mykeystore.jks")
    val configuration = sslConfigurationProvider.getSslConfiguration()
    assert(!configuration.isKeyStoreLocalFile, "KeyStore should not be a local file.")
    assert(configuration.sslOptions.keyStore.isDefined, "KeyStore file should be defined.")
    configuration.sslOptions.keyStore.foreach { returnedStore =>
      assert(returnedStore.getAbsolutePath === "/opt/spark/mykeystore.jks",
        "Resolved KeyStore path should match the path of the input URI.")
    }
    val expectedSecret = new SecretBuilder()
        .withNewMetadata()
        .withName(s"$SUBMISSION_SSL_SECRETS_PREFIX-$APP_ID")
        .endMetadata()
        .withType("Opaque")
        .build()
    assert(configuration.sslSecrets.toSeq === Seq(expectedSecret),
      "SSL Secret was incorrect (it should have no data).")
    verify(kubernetesClient).secrets()
    verify(secretsHandler).create(expectedSecret)
  }
}
