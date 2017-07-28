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
package org.apache.spark.deploy.kubernetes.integrationtest

import java.nio.file.Paths
import java.util.UUID

import io.fabric8.kubernetes.client.internal.readiness.Readiness

import org.apache.spark.{SparkConf, SSLOptions, SparkFunSuite}

import org.apache.spark.deploy.kubernetes.config._

import org.apache.spark.deploy.kubernetes.integrationtest.backend.IntegrationTestBackendFactory
import org.apache.spark.deploy.kubernetes.integrationtest.backend.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.constants.MINIKUBE_TEST_BACKEND
import org.apache.spark.deploy.kubernetes.submit._

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.collection.JavaConverters._

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {
  import KubernetesSuite._
  private val testBackend = IntegrationTestBackendFactory.getTestBackend()
  private val APP_LOCATOR_LABEL = UUID.randomUUID().toString.replaceAll("-", "")
  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkConf: SparkConf = _
  private var resourceStagingServerLauncher: ResourceStagingServerLauncher = _
  private var staticAssetServerLauncher: StaticAssetServerLauncher = _
  private var kerberizedHadoopClusterLauncher: KerberizedHadoopClusterLauncher = _
  private var kerberosTestLauncher: KerberosTestPodLauncher = _

  override def beforeAll(): Unit = {
    testBackend.initialize()
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
    resourceStagingServerLauncher = new ResourceStagingServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
    staticAssetServerLauncher = new StaticAssetServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
    kerberizedHadoopClusterLauncher = new KerberizedHadoopClusterLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace),
      kubernetesTestComponents.namespace)
    kerberosTestLauncher = new KerberosTestPodLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace),
      kubernetesTestComponents.namespace)
  }

  override def afterAll(): Unit = {
    testBackend.cleanUp()
  }

  before {
    sparkConf = kubernetesTestComponents.newSparkConf()
      .set(INIT_CONTAINER_DOCKER_IMAGE, s"spark-init:latest")
      .set(DRIVER_DOCKER_IMAGE, s"spark-driver:latest")
      .set(KUBERNETES_DRIVER_LABELS, s"spark-app-locator=$APP_LOCATOR_LABEL")
    kubernetesTestComponents.createNamespace()
  }

  after {
    kubernetesTestComponents.deleteKubernetesResources()
    // kubernetesTestComponents.deleteNamespace()
  }

//  test("Include HADOOP_CONF for HDFS based jobs") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    // Ensuring that HADOOP_CONF_DIR variable is set, could also be one via env HADOOP_CONF_DIR
//    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
//    runSparkApplicationAndVerifyCompletion(
//      JavaMainAppResource(CONTAINER_LOCAL_MAIN_APP_RESOURCE),
//      SPARK_PI_MAIN_CLASS,
//      Seq("HADOOP_CONF_DIR defined. Mounting HDFS specific .xml files", "Pi is roughly 3"),
//      Array("5"),
//      Seq.empty[String],
//      Some("src/test/resources"))
//  }

  test("Secure HDFS test with HDFS keytab") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    launchKerberizedCluster()
    createKerberosTestPod(CONTAINER_LOCAL_MAIN_APP_RESOURCE, HDFS_TEST_CLASS, APP_LOCATOR_LABEL)
    val expectedLogOnCompletion = Seq("Something something something")
    Thread.sleep(50000)
//    val driverPod = kubernetesTestComponents.kubernetesClient
//      .pods()
//      .withLabel("spark-app-locator", APP_LOCATOR_LABEL)
//      .list()
//      .getItems
//      .get(0)
//    Eventually.eventually(TIMEOUT, INTERVAL) {
//      expectedLogOnCompletion.foreach { e =>
//        assert(kubernetesTestComponents.kubernetesClient
//          .pods()
//          .withName(driverPod.getMetadata.getName)
//          .getLog
//          .contains(e), "The application did not complete.")
//      }
//    }
  }

//  test("Run PySpark Job on file from SUBMITTER with --py-files") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    launchStagingServer(SSLOptions(), None)
//    sparkConf
//      .set(DRIVER_DOCKER_IMAGE,
//        System.getProperty("spark.docker.test.driverImage", "spark-driver-py:latest"))
//      .set(EXECUTOR_DOCKER_IMAGE,
//        System.getProperty("spark.docker.test.executorImage", "spark-executor-py:latest"))
//
//    runPySparkPiAndVerifyCompletion(
//      PYSPARK_PI_SUBMITTER_LOCAL_FILE_LOCATION,
//      Seq(PYSPARK_SORT_CONTAINER_LOCAL_FILE_LOCATION)
//    )
//  }
//
//  test("Run PySpark Job on file from CONTAINER with spark.jar defined") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
//    sparkConf
//      .set(DRIVER_DOCKER_IMAGE,
//      System.getProperty("spark.docker.test.driverImage", "spark-driver-py:latest"))
//      .set(EXECUTOR_DOCKER_IMAGE,
//      System.getProperty("spark.docker.test.executorImage", "spark-executor-py:latest"))
//
//    runPySparkPiAndVerifyCompletion(PYSPARK_PI_CONTAINER_LOCAL_FILE_LOCATION, Seq.empty[String])
//  }
//
//  test("Simple submission test with the resource staging server.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    launchStagingServer(SSLOptions(), None)
//    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
//  }
//
//  test("Enable SSL on the resource staging server") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    val keyStoreAndTrustStore = SSLUtils.generateKeyStoreTrustStorePair(
//      ipAddress = Minikube.getMinikubeIp,
//      keyStorePassword = "keyStore",
//      keyPassword = "key",
//      trustStorePassword = "trustStore")
//    sparkConf.set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
//      .set("spark.ssl.kubernetes.resourceStagingServer.keyStore",
//          keyStoreAndTrustStore.keyStore.getAbsolutePath)
//      .set("spark.ssl.kubernetes.resourceStagingServer.trustStore",
//          keyStoreAndTrustStore.trustStore.getAbsolutePath)
//      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", "keyStore")
//      .set("spark.ssl.kubernetes.resourceStagingServer.keyPassword", "key")
//      .set("spark.ssl.kubernetes.resourceStagingServer.trustStorePassword", "trustStore")
//    launchStagingServer(SSLOptions(
//      enabled = true,
//      keyStore = Some(keyStoreAndTrustStore.keyStore),
//      trustStore = Some(keyStoreAndTrustStore.trustStore),
//      keyStorePassword = Some("keyStore"),
//      keyPassword = Some("key"),
//      trustStorePassword = Some("trustStore")),
//      None)
//    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
//  }
//
//  test("Use container-local resources without the resource staging server") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
//    runSparkPiAndVerifyCompletion(CONTAINER_LOCAL_MAIN_APP_RESOURCE)
//  }
//
//  test("Dynamic executor scaling basic test") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    launchStagingServer(SSLOptions(), None)
//    createShuffleServiceDaemonSet()
//
//    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
//    sparkConf.set("spark.dynamicAllocation.enabled", "true")
//    sparkConf.set("spark.shuffle.service.enabled", "true")
//    sparkConf.set("spark.kubernetes.shuffle.labels", "app=spark-shuffle-service")
//    sparkConf.set("spark.kubernetes.shuffle.namespace", kubernetesTestComponents.namespace)
//    sparkConf.set("spark.app.name", "group-by-test")
//    runSparkApplicationAndVerifyCompletion(
//        JavaMainAppResource(SUBMITTER_LOCAL_MAIN_APP_RESOURCE),
//        GROUP_BY_MAIN_CLASS,
//        Seq("The Result is"),
//        Array.empty[String],
//        Seq.empty[String],
//        None)
//  }
//
//  test("Use remote resources without the resource staging server.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
//    sparkConf.setJars(Seq(
//      s"$assetServerUri/${EXAMPLES_JAR_FILE.getName}",
//      s"$assetServerUri/${HELPER_JAR_FILE.getName}"
//    ))
//    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
//  }
//
//  test("Mix remote resources with submitted ones.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    launchStagingServer(SSLOptions(), None)
//    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
//    sparkConf.setJars(Seq(
//      SUBMITTER_LOCAL_MAIN_APP_RESOURCE, s"$assetServerUri/${HELPER_JAR_FILE.getName}"
//    ))
//    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
//  }
//
//  test("Use key and certificate PEM files for TLS.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    val keyAndCertificate = SSLUtils.generateKeyCertPemPair(Minikube.getMinikubeIp)
//    launchStagingServer(
//        SSLOptions(enabled = true),
//        Some(keyAndCertificate))
//    sparkConf.set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
//        .set(
//            RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM.key, keyAndCertificate.certPem.getAbsolutePath)
//    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
//  }
//
//  test("Use client key and client cert file when requesting executors") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    sparkConf.setJars(Seq(
//        CONTAINER_LOCAL_MAIN_APP_RESOURCE,
//        CONTAINER_LOCAL_HELPER_JAR_PATH))
//    sparkConf.set(
//        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
//        kubernetesTestComponents.clientConfig.getClientKeyFile)
//    sparkConf.set(
//        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
//        kubernetesTestComponents.clientConfig.getClientCertFile)
//    sparkConf.set(
//        s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
//        kubernetesTestComponents.clientConfig.getCaCertFile)
//    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
//  }
//
//  test("Added files should be placed in the driver's working directory.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//    val testExistenceFileTempDir = Utils.createTempDir(namePrefix = "test-existence-file-temp-dir")
//    val testExistenceFile = new File(testExistenceFileTempDir, "input.txt")
//    Files.write(TEST_EXISTENCE_FILE_CONTENTS, testExistenceFile, Charsets.UTF_8)
//    launchStagingServer(SSLOptions(), None)
//    sparkConf.set("spark.files", testExistenceFile.getAbsolutePath)
//    runSparkApplicationAndVerifyCompletion(
//        JavaMainAppResource(SUBMITTER_LOCAL_MAIN_APP_RESOURCE),
//        FILE_EXISTENCE_MAIN_CLASS,
//        Seq(s"File found at /opt/spark/${testExistenceFile.getName} with correct contents."),
//        Array(testExistenceFile.getName, TEST_EXISTENCE_FILE_CONTENTS),
//        Seq.empty[String],
//        None)
//  }
//
//  test("Use a very long application name.") {
//    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
//
//    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH)).setAppName("long" * 40)
//    runSparkPiAndVerifyCompletion(CONTAINER_LOCAL_MAIN_APP_RESOURCE)
//  }

  private def launchStagingServer(
      resourceStagingServerSslOptions: SSLOptions, keyAndCertPem: Option[KeyAndCertPem]): Unit = {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    val resourceStagingServerPort = resourceStagingServerLauncher.launchStagingServer(
      resourceStagingServerSslOptions, keyAndCertPem)
    val resourceStagingServerUriScheme = if (resourceStagingServerSslOptions.enabled) {
      "https"
    } else {
      "http"
    }
    sparkConf.set(RESOURCE_STAGING_SERVER_URI,
      s"$resourceStagingServerUriScheme://" +
        s"${Minikube.getMinikubeIp}:$resourceStagingServerPort")
  }

  private def launchKerberizedCluster(): Unit = {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    kerberizedHadoopClusterLauncher.launchKerberizedCluster()
  }

  private def createKerberosTestPod(resource: String, className: String, appLabel: String): Unit = {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    kerberosTestLauncher.startKerberosTest(resource, className, appLabel)
  }

  private def runSparkPiAndVerifyCompletion(appResource: String): Unit = {
    runSparkApplicationAndVerifyCompletion(
        JavaMainAppResource(appResource),
        SPARK_PI_MAIN_CLASS,
        Seq(
          "hadoop config map key was not specified",
          "Pi is roughly 3"),
        Array.empty[String],
        Seq.empty[String],
        None)
  }

  private def runPySparkPiAndVerifyCompletion(
      appResource: String, otherPyFiles: Seq[String]): Unit = {
    runSparkApplicationAndVerifyCompletion(
      PythonMainAppResource(appResource),
      PYSPARK_PI_MAIN_CLASS,
      Seq("Submitting 5 missing tasks from ResultStage", "Pi is roughly 3"),
      Array("5"),
      otherPyFiles,
      None)
  }

  private def runSparkApplicationAndVerifyCompletion(
      appResource: MainAppResource,
      mainClass: String,
      expectedLogOnCompletion: Seq[String],
      appArgs: Array[String],
      otherPyFiles: Seq[String],
      hadoopConfDir: Option[String]): Unit = {
    val clientArguments = ClientArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      driverArgs = appArgs,
      otherPyFiles = otherPyFiles)
    Client.run(sparkConf, clientArguments, hadoopConfDir)
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", APP_LOCATOR_LABEL)
      .list()
      .getItems
      .get(0)
    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
  }

  private def createShuffleServiceDaemonSet(): Unit = {
    val ds = kubernetesTestComponents.kubernetesClient.extensions().daemonSets()
      .createNew()
        .withNewMetadata()
        .withName("shuffle")
      .endMetadata()
      .withNewSpec()
        .withNewTemplate()
          .withNewMetadata()
            .withLabels(Map("app" -> "spark-shuffle-service").asJava)
          .endMetadata()
          .withNewSpec()
            .addNewVolume()
              .withName("shuffle-dir")
              .withNewHostPath()
                .withPath("/tmp")
              .endHostPath()
            .endVolume()
            .addNewContainer()
              .withName("shuffle")
              .withImage("spark-shuffle:latest")
              .withImagePullPolicy("IfNotPresent")
              .addNewVolumeMount()
                .withName("shuffle-dir")
                .withMountPath("/tmp")
              .endVolumeMount()
            .endContainer()
          .endSpec()
        .endTemplate()
      .endSpec()
      .done()

    // wait for daemonset to become available.
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val pods = kubernetesTestComponents.kubernetesClient.pods()
        .withLabel("app", "spark-shuffle-service").list().getItems

      if (pods.size() == 0 || !Readiness.isReady(pods.get(0))) {
        throw ShuffleNotReadyException
      }
    }
  }
}

private[spark] object KubernetesSuite {
  val EXAMPLES_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs")
    .toFile
    .listFiles()(0)

  val HELPER_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs-helpers")
    .toFile
    .listFiles()(0)
  val SUBMITTER_LOCAL_MAIN_APP_RESOURCE = s"file://${EXAMPLES_JAR_FILE.getAbsolutePath}"
  val CONTAINER_LOCAL_MAIN_APP_RESOURCE = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${EXAMPLES_JAR_FILE.getName}"
  val CONTAINER_LOCAL_HELPER_JAR_PATH = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${HELPER_JAR_FILE.getName}"
  val TIMEOUT = PatienceConfiguration.Timeout(Span(20, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(20, Seconds))
  val SPARK_PI_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  val PYSPARK_PI_MAIN_CLASS = "org.apache.spark.deploy.PythonRunner"
  val PYSPARK_PI_CONTAINER_LOCAL_FILE_LOCATION =
    "local:///opt/spark/examples/src/main/python/pi.py"
  val PYSPARK_SORT_CONTAINER_LOCAL_FILE_LOCATION =
    "local:///opt/spark/examples/src/main/python/sort.py"
  val PYSPARK_PI_SUBMITTER_LOCAL_FILE_LOCATION = "src/test/python/pi.py"
  val FILE_EXISTENCE_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.FileExistenceTest"
  val GROUP_BY_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.GroupByTest"
  val HDFS_TEST_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.HDFSTest"
  val TEST_EXISTENCE_FILE_CONTENTS = "contents"


  case object ShuffleNotReadyException extends Exception
}
