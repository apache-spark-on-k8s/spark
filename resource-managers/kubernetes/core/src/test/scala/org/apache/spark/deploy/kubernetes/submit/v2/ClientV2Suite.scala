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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.File

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, Container, DoneablePod, HasMetadata, Pod, PodBuilder, PodList, Secret, SecretBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.{AdditionalAnswers, Mockito}
import org.mockito.Matchers.{any, anyVararg, argThat, eq => mockitoEq, startsWith}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.kubernetes.v2.StagedResourceIdentifier
import org.apache.spark.util.Utils

class ClientV2Suite extends SparkFunSuite with BeforeAndAfter {

  private val MAIN_CLASS = "org.apache.spark.test.Main"
  private val APP_ARGS = Array[String]("arg1", "arg2")
  private val MAIN_APP_RESOURCE = "local:///app/jars/spark-main.jar"
  private val APP_NAME = "spark-test-app"
  private val STAGING_SERVER_URI = "http://localhost:9000"
  private val SPARK_JARS = Seq(
    "local:///app/jars/spark-helper.jar", "file:///var/data/spark-local-helper.jar")
  private val RESOLVED_SPARK_JARS = Seq(
    "local:///app/jars/spark-helper.jar",
    "file:///var/data/spark-downloaded/spark-local-helper.jar")
  private val SPARK_FILES = Seq(
    "local:///app/files/spark-file.txt", "file:///var/data/spark-local-file.txt")
  private val RESOLVED_SPARK_FILES = Seq(
    "local:///app/files/spark-file.txt", "file:///var/data/spark-downloaded/spark-local-file.txt")
  private val DRIVER_EXTRA_CLASSPATH = "/app/jars/extra-jar1.jar:/app/jars/extra-jars2.jar"
  private val DRIVER_DOCKER_IMAGE_VALUE = "spark-driver:latest"
  private val DRIVER_MEMORY_OVERHEARD_MB = 128L
  private val DRIVER_MEMORY_MB = 512L
  private val NAMESPACE = "namespace"
  private val DOWNLOAD_JARS_RESOURCE_IDENTIFIER = StagedResourceIdentifier("jarsId", "jarsSecret")
  private val DOWNLOAD_FILES_RESOURCE_IDENTIFIER = StagedResourceIdentifier(
    "filesId", "filesSecret")
  private val MOUNTED_FILES_ANNOTATION_KEY = "mountedFiles"

  private val downloadRemoteDependenciesConfigMap = new ConfigMapBuilder()
    .withNewMetadata().withName("init-container").endMetadata()
    .addToData("key", "value")
    .build()

  private var sparkConf: SparkConf = _
  private var submissionKubernetesClientProvider: SubmissionKubernetesClientProvider = _
  private var submissionKubernetesClient: KubernetesClient = _
  private type PODS = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
  private type RESOURCES = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
    HasMetadata, Boolean]
  private var podOperations: PODS = _
  private var resourceListOperations: RESOURCES = _
  private var submittedDependencyManagerProvider: SubmittedDependencyManagerProvider = _
  private var remoteDependencyManagerProvider: DownloadRemoteDependencyManagerProvider = _
  private var remoteDependencyManager: DownloadRemoteDependencyManager = _
  private var mountedDependencyManager: SubmittedDependencyManager = _
  private var captureCreatedPodAnswer: SelfArgumentCapturingAnswer[Pod] = _
  private var captureCreatedResourcesAnswer: AllArgumentsCapturingAnswer[HasMetadata, RESOURCES] = _
  private var credentialsMounterProvider: DriverPodKubernetesCredentialsMounterProvider = _
  private var credentialsMounter: DriverPodKubernetesCredentialsMounter = _
  private var submittedFilesInitContainerBootstrap: SparkPodInitContainerBootstrap = _
  private var downloadRemoteFilesInitContainerBootstrap: SparkPodInitContainerBootstrap = _
  private var capturedJars: Option[Seq[String]] = None
  private var capturedFiles: Option[Seq[String]] = None

  before {
    capturedJars = None
    capturedFiles = None
    sparkConf = new SparkConf(true)
      .set("spark.app.name", APP_NAME)
      .set("spark.master", "k8s://https://localhost:443")
      .set(DRIVER_DOCKER_IMAGE, DRIVER_DOCKER_IMAGE_VALUE)
      .set(KUBERNETES_DRIVER_MEMORY_OVERHEAD, DRIVER_MEMORY_OVERHEARD_MB)
      .set(KUBERNETES_NAMESPACE, NAMESPACE)
      .set(org.apache.spark.internal.config.DRIVER_MEMORY, DRIVER_MEMORY_MB)
    submissionKubernetesClientProvider = mock[SubmissionKubernetesClientProvider]
    submissionKubernetesClient = mock[KubernetesClient]
    podOperations = mock[PODS]
    resourceListOperations = mock[RESOURCES]
    submittedDependencyManagerProvider = mock[SubmittedDependencyManagerProvider]
    mountedDependencyManager = mock[SubmittedDependencyManager]
    remoteDependencyManagerProvider = mock[DownloadRemoteDependencyManagerProvider]
    remoteDependencyManager = mock[DownloadRemoteDependencyManager]
    credentialsMounterProvider = mock[DriverPodKubernetesCredentialsMounterProvider]
    credentialsMounter = mock[DriverPodKubernetesCredentialsMounter]
    submittedFilesInitContainerBootstrap = mock[SparkPodInitContainerBootstrap]
    downloadRemoteFilesInitContainerBootstrap = mock[SparkPodInitContainerBootstrap]
    when(remoteDependencyManagerProvider.getDownloadRemoteDependencyManager(any(), any(), any()))
      .thenAnswer(new Answer[DownloadRemoteDependencyManager] {
        override def answer(invocationOnMock: InvocationOnMock): DownloadRemoteDependencyManager = {
          capturedJars = Some(invocationOnMock.getArgumentAt(1, classOf[Seq[String]]))
          capturedFiles = Some(invocationOnMock.getArgumentAt(2, classOf[Seq[String]]))
          remoteDependencyManager
        }
      })
    when(remoteDependencyManager.buildInitContainerConfigMap())
      .thenReturn(downloadRemoteDependenciesConfigMap)
    when(remoteDependencyManager.resolveLocalClasspath()).thenAnswer(new Answer[Seq[String]] {
      override def answer(invocationOnMock: InvocationOnMock): Seq[String] = {
        assert(capturedJars.isDefined)
        capturedJars.toSeq.flatten.map(Utils.resolveURI(_).getPath)
      }
    })
    when(remoteDependencyManager.getInitContainerBootstrap(downloadRemoteDependenciesConfigMap))
      .thenReturn(downloadRemoteFilesInitContainerBootstrap)
    when(downloadRemoteFilesInitContainerBootstrap.bootstrapInitContainerAndVolumes(
        mockitoEq(DRIVER_CONTAINER_NAME), any()))
      .thenAnswer(AdditionalAnswers.returnsArgAt(1))
    when(remoteDependencyManager.configureExecutorsToFetchRemoteDependencies(any(), any()))
      .thenAnswer(AdditionalAnswers.returnsArgAt(0))
    when(submissionKubernetesClientProvider.get).thenReturn(submissionKubernetesClient)
    when(submissionKubernetesClient.pods()).thenReturn(podOperations)
    captureCreatedPodAnswer = new SelfArgumentCapturingAnswer[Pod]
    captureCreatedResourcesAnswer = new AllArgumentsCapturingAnswer[HasMetadata, RESOURCES](
      resourceListOperations)
    when(podOperations.create(any())).thenAnswer(captureCreatedPodAnswer)
    when(submissionKubernetesClient.resourceList(anyVararg[HasMetadata]))
      .thenAnswer(captureCreatedResourcesAnswer)
    when(credentialsMounterProvider.getDriverPodKubernetesCredentialsMounter(any()))
      .thenReturn(credentialsMounter)
    when(credentialsMounter.mountDriverKubernetesCredentials(any(), any(), any()))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(credentialsMounter.setDriverPodKubernetesCredentialLocations(any()))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(credentialsMounter.createCredentialsSecret()).thenReturn(None)
  }

  // Tests w/o local dependencies, or behave independently to that configuration.
  test("Simple properties and environment set on the driver pod.") {
    sparkConf.set(org.apache.spark.internal.config.DRIVER_CLASS_PATH, DRIVER_EXTRA_CLASSPATH)
    val createdDriverPod = createAndGetDriverPod()
    val maybeDriverContainer = getDriverContainer(createdDriverPod)
    maybeDriverContainer.foreach { driverContainer =>
      assert(driverContainer.getName === DRIVER_CONTAINER_NAME)
      assert(driverContainer.getImage === DRIVER_DOCKER_IMAGE_VALUE)
      assert(driverContainer.getImagePullPolicy === "IfNotPresent")
      val envs = driverContainer.getEnv.asScala.map { env =>
        (env.getName, env.getValue)
      }.toMap
      assert(envs(ENV_DRIVER_MEMORY) === s"${DRIVER_MEMORY_MB + DRIVER_MEMORY_OVERHEARD_MB}m")
      assert(envs(ENV_DRIVER_MAIN_CLASS) === MAIN_CLASS)
      assert(envs(ENV_DRIVER_ARGS) === APP_ARGS.mkString(" "))
      assert(envs(ENV_SUBMIT_EXTRA_CLASSPATH) === DRIVER_EXTRA_CLASSPATH)
    }
  }

  test("Created pod should apply custom annotations and labels") {
    sparkConf.set(KUBERNETES_DRIVER_LABELS,
      "label1=label1value,label2=label2value")
    sparkConf.set(KUBERNETES_DRIVER_ANNOTATIONS,
      "annotation1=annotation1value,annotation2=annotation2value")
    val createdDriverPod = createAndGetDriverPod()
    val labels = createdDriverPod.getMetadata.getLabels.asScala
    assert(labels.size === 4)
    // App ID is non-deterministic, but just check if it's set and is prefixed with the app name
    val appIdLabel = labels(SPARK_APP_ID_LABEL)
    assert(appIdLabel != null && appIdLabel.startsWith(APP_NAME) && appIdLabel != APP_NAME)
    val appNameLabel = labels(SPARK_APP_NAME_LABEL)
    assert(appNameLabel != null && appNameLabel == APP_NAME)
    assert(labels("label1") === "label1value")
    assert(labels("label2") === "label2value")
    val annotations = createdDriverPod.getMetadata.getAnnotations.asScala
    val expectedAnnotations = Map(
      "annotation1" -> "annotation1value", "annotation2" -> "annotation2value")
    assert(annotations === expectedAnnotations)
  }

  test("Driver JVM Options should be set in the environment.") {
    sparkConf.set(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS, "-Dopt1=opt1value")
    sparkConf.set("spark.logConf", "true")
    val createdDriverPod = createAndGetDriverPod()
    val maybeJvmOptions = getDriverJvmOptions(createdDriverPod)
    assert(maybeJvmOptions.isDefined)
    maybeJvmOptions.foreach { jvmOptions =>
      assert(jvmOptions("opt1") === "opt1value")
      assert(jvmOptions.contains("spark.app.id"))
      assert(jvmOptions("spark.jars") === MAIN_APP_RESOURCE)
      assert(jvmOptions(KUBERNETES_DRIVER_POD_NAME.key).startsWith(APP_NAME))
      assert(jvmOptions("spark.app.name") === APP_NAME)
      assert(jvmOptions("spark.logConf") === "true")
    }
  }

  // Tests with local dependencies with the mounted dependency manager.
  private def getDriverJvmOptions(driverPod: Pod): Option[Map[String, String]] = {
    val maybeDriverContainer = getDriverContainer(driverPod)
    val maybeJvmOptions = maybeDriverContainer.flatMap { driverContainer =>
      val maybeJvmOptionsEnv = driverContainer.getEnv
        .asScala
        .find(_.getName == ENV_DRIVER_JAVA_OPTS)
      maybeJvmOptionsEnv.map { jvmOptionsEnv =>
        val jvmOptions = jvmOptionsEnv.getValue.split(" ")
        jvmOptions.foreach { opt => assert(opt.startsWith("-D")) }
        jvmOptions.map { option =>
          val withoutDashDPrefix = option.stripPrefix("-D")
          val split = withoutDashDPrefix.split('=')
          assert(split.length == 2)
          (split(0), split(1))
        }.toMap
      }
    }
    maybeJvmOptions
  }

  test("Uploading local dependencies should configure executors to pull from the" +
    " resource staging server") {
    val initContainerConfigMap = getInitContainerConfigMap
    val initContainerSecret = getInitContainerSecret
    runWithMountedDependencies(initContainerConfigMap, initContainerSecret)
    val driverPod = captureCreatedPodAnswer.capturedArgument
    val jvmOptions = getDriverJvmOptions(driverPod)
    assert(jvmOptions.isDefined)
    jvmOptions.foreach { options =>
      assert(options("spark.testing.configuredExecutorsMountLocal") === "true")
    }
  }

  test("Uploading local dependencies should create Kubernetes secrets and config map") {
    val initContainerConfigMap = getInitContainerConfigMap
    val initContainerSecret = getInitContainerSecret
    runWithMountedDependencies(initContainerConfigMap, initContainerSecret)
    val driverPod = captureCreatedPodAnswer.capturedArgument
    assert(captureCreatedResourcesAnswer.capturedArguments != null)
    assert(captureCreatedResourcesAnswer.capturedArguments.size === 3)
    assert(captureCreatedResourcesAnswer.capturedArguments.toSet ===
      Set(initContainerSecret, initContainerConfigMap, downloadRemoteDependenciesConfigMap))
    captureCreatedResourcesAnswer.capturedArguments.foreach { resource =>
      val driverPodOwnerReferences = resource.getMetadata.getOwnerReferences
      assert(driverPodOwnerReferences.size === 1)
      val driverPodOwnerReference = driverPodOwnerReferences.asScala.head
      assert(driverPodOwnerReference.getName === driverPod.getMetadata.getName)
      assert(driverPodOwnerReference.getApiVersion === driverPod.getApiVersion)
      assert(driverPodOwnerReference.getUid === driverPod.getMetadata.getUid)
      assert(driverPodOwnerReference.getKind === driverPod.getKind)
      assert(driverPodOwnerReference.getController)
    }
  }

  test("Uploading local resources should set classpath environment variables") {
    val initContainerConfigMap = getInitContainerConfigMap
    val initContainerSecret = getInitContainerSecret
    runWithMountedDependencies(initContainerConfigMap, initContainerSecret)
    val driverPod = captureCreatedPodAnswer.capturedArgument
    val maybeDriverContainer = getDriverContainer(driverPod)
    maybeDriverContainer.foreach { driverContainer =>
      val envs = driverContainer.getEnv
        .asScala
        .map { env => (env.getName, env.getValue) }
        .toMap
      val classPathEntries = envs(ENV_MOUNTED_CLASSPATH).split(File.pathSeparator).toSet
      val expectedClassPathEntries = RESOLVED_SPARK_JARS
        .map(Utils.resolveURI)
        .map(_.getPath)
        .toSet
      assert(classPathEntries === expectedClassPathEntries)
    }
  }

  test("Remote dependency manager should configure the driver pod and the local classpath") {
    Mockito.reset(remoteDependencyManager, downloadRemoteFilesInitContainerBootstrap)
    when(remoteDependencyManager.configureExecutorsToFetchRemoteDependencies(any(), any()))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(remoteDependencyManager.getInitContainerBootstrap(downloadRemoteDependenciesConfigMap))
      .thenReturn(downloadRemoteFilesInitContainerBootstrap)
    when(downloadRemoteFilesInitContainerBootstrap.bootstrapInitContainerAndVolumes(
          mockitoEq(DRIVER_CONTAINER_NAME), any()))
        .thenAnswer(new Answer[PodBuilder]() {
      override def answer(invocationOnMock: InvocationOnMock): PodBuilder = {
        val originalPod = invocationOnMock.getArgumentAt(1, classOf[PodBuilder])
        originalPod.editMetadata().addToLabels("added-remote-dependency", "true").endMetadata()
      }
    })
    when(remoteDependencyManager.resolveLocalClasspath())
      .thenReturn(Seq("/app/jars/resolved-jar-1.jar", "/app/jars/resolved-jar-2.jar"))
    when(remoteDependencyManager.buildInitContainerConfigMap())
      .thenReturn(downloadRemoteDependenciesConfigMap)
    val createdDriverPod = createAndGetDriverPod()
    Mockito.verify(remoteDependencyManager)
      .getInitContainerBootstrap(downloadRemoteDependenciesConfigMap)
    Mockito.verify(downloadRemoteFilesInitContainerBootstrap).bootstrapInitContainerAndVolumes(
      mockitoEq(DRIVER_CONTAINER_NAME),
      any())
    assert(createdDriverPod.getMetadata.getLabels.get("added-remote-dependency") === "true")
    val driverContainer = createdDriverPod
      .getSpec
      .getContainers
      .asScala
      .find(_.getName == DRIVER_CONTAINER_NAME)
    assert(driverContainer.isDefined)
    driverContainer.foreach { container =>
      val env = container.getEnv.asScala
      val mountedClasspathEnv = env.find(_.getName == ENV_MOUNTED_CLASSPATH)
      assert(mountedClasspathEnv.isDefined)
      mountedClasspathEnv.foreach { classpathEnv =>
        assert(classpathEnv.getValue ===
          "/app/jars/resolved-jar-1.jar:/app/jars/resolved-jar-2.jar")
      }
    }
  }

  private def getInitContainerSecret: Secret = {
    new SecretBuilder()
      .withNewMetadata().withName(s"$APP_NAME-init-container-secret").endMetadata()
      .addToData(
        INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_JARS_SECRET_KEY,
        DOWNLOAD_JARS_RESOURCE_IDENTIFIER.resourceSecret)
      .addToData(INIT_CONTAINER_SUBMITTED_FILES_DOWNLOAD_FILES_SECRET_KEY,
        DOWNLOAD_FILES_RESOURCE_IDENTIFIER.resourceSecret)
      .build()
  }

  private def getInitContainerConfigMap: ConfigMap = {
    new ConfigMapBuilder()
      .withNewMetadata().withName(s"$APP_NAME-init-container-conf").endMetadata()
      .addToData("key", "configuration")
      .build()
  }

  private def runWithMountedDependencies(
      initContainerConfigMap: ConfigMap, initContainerSecret: Secret): Unit = {
    sparkConf.set(RESOURCE_STAGING_SERVER_URI, STAGING_SERVER_URI)
      .setJars(SPARK_JARS)
      .set("spark.files", SPARK_FILES.mkString(","))
    val labelsMatcher = new BaseMatcher[Map[String, String]] {
      override def matches(maybeLabels: scala.Any) = {
        maybeLabels match {
          case labels: Map[String, String] =>
            labels(SPARK_APP_ID_LABEL).startsWith(APP_NAME) &&
              labels(SPARK_APP_NAME_LABEL) == APP_NAME
          case _ => false
        }
      }

      override def describeTo(description: Description) = {
        description.appendText("Checks if the labels contain the app ID and app name.")
      }
    }
    when(submittedDependencyManagerProvider.getSubmittedDependencyManager(
      startsWith(APP_NAME),
      mockitoEq(STAGING_SERVER_URI),
      argThat(labelsMatcher),
      mockitoEq(NAMESPACE),
      mockitoEq(SPARK_JARS ++ Seq(MAIN_APP_RESOURCE)),
      mockitoEq(SPARK_FILES))).thenReturn(mountedDependencyManager)
    when(mountedDependencyManager.uploadJars()).thenReturn(DOWNLOAD_JARS_RESOURCE_IDENTIFIER)
    when(mountedDependencyManager.uploadFiles()).thenReturn(DOWNLOAD_FILES_RESOURCE_IDENTIFIER)
    when(mountedDependencyManager.buildInitContainerSecret(
      DOWNLOAD_JARS_RESOURCE_IDENTIFIER.resourceSecret,
      DOWNLOAD_FILES_RESOURCE_IDENTIFIER.resourceSecret))
      .thenReturn(initContainerSecret)
    when(mountedDependencyManager.buildInitContainerConfigMap(
      DOWNLOAD_JARS_RESOURCE_IDENTIFIER.resourceId, DOWNLOAD_FILES_RESOURCE_IDENTIFIER.resourceId))
      .thenReturn(initContainerConfigMap)
    when(mountedDependencyManager.configureExecutorsToFetchSubmittedDependencies(
        any(), any(), any()))
      .thenAnswer(new Answer[SparkConf]() {
        override def answer(invocationOnMock: InvocationOnMock): SparkConf = {
          val originalSparkConf = invocationOnMock.getArgumentAt(0, classOf[SparkConf])
          originalSparkConf.clone().set("spark.testing.configuredExecutorsMountLocal", "true")
        }
      })
    when(mountedDependencyManager.resolveSparkJars()).thenReturn(RESOLVED_SPARK_JARS)
    when(mountedDependencyManager.resolveSparkFiles()).thenReturn(RESOLVED_SPARK_FILES)
    when(mountedDependencyManager.getInitContainerBootstrap(
      initContainerSecret,
      initContainerConfigMap)).thenReturn(submittedFilesInitContainerBootstrap)
    when(submittedFilesInitContainerBootstrap.bootstrapInitContainerAndVolumes(
        mockitoEq(DRIVER_CONTAINER_NAME), any()))
      .thenAnswer(new Answer[PodBuilder]() {
        override def answer(invocationOnMock: InvocationOnMock): PodBuilder = {
          val basePod = invocationOnMock.getArgumentAt(1, classOf[PodBuilder])
          basePod.editMetadata()
            .addToAnnotations(MOUNTED_FILES_ANNOTATION_KEY, "true")
            .endMetadata()
        }
      })
    createAndGetDriverPod()
  }

  private def getDriverContainer(driverPod: Pod): Option[Container] = {
    val maybeDriverContainer = driverPod.getSpec
      .getContainers
      .asScala
      .find(_.getName == DRIVER_CONTAINER_NAME)
    assert(maybeDriverContainer.isDefined)
    maybeDriverContainer
  }

  private def createAndGetDriverPod(): Pod = {
    val clientUnderTest = createClient()
    clientUnderTest.run()
    val createdDriverPod = captureCreatedPodAnswer.capturedArgument
    assert(createdDriverPod != null)
    createdDriverPod
  }

  private def createClient(): Client = {
    new Client(
      MAIN_CLASS,
      sparkConf,
      APP_ARGS,
      MAIN_APP_RESOURCE,
      submissionKubernetesClientProvider,
      submittedDependencyManagerProvider,
      remoteDependencyManagerProvider,
      credentialsMounterProvider)
  }

  private class SelfArgumentCapturingAnswer[T: ClassTag] extends Answer[T] {
    var capturedArgument: T = _

    override def answer(invocationOnMock: InvocationOnMock): T = {
      val argumentClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val argument = invocationOnMock.getArgumentAt(0, argumentClass)
      this.capturedArgument = argument
      argument
    }
  }

  private class AllArgumentsCapturingAnswer[I, T](returnValue: T) extends Answer[T] {
    var capturedArguments: Seq[I] = _

    override def answer(invocationOnMock: InvocationOnMock): T = {
      capturedArguments = invocationOnMock.getArguments.map(_.asInstanceOf[I]).toSeq
      returnValue
    }
  }
}
