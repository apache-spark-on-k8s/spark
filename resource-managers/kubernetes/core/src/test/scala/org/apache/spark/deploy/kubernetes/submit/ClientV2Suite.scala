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
package org.apache.spark.deploy.kubernetes.submit

import java.io.File

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.{AdditionalAnswers, ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, anyVararg, argThat, eq => mockitoEq}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfter, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.{KubernetesExternalShuffleService, KubernetesShuffleBlockHandler, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClient

class ClientV2Suite extends SparkFunSuite with BeforeAndAfter {
  private val JARS_RESOURCE = SubmittedResourceIdAndSecret("jarsId", "jarsSecret")
  private val FILES_RESOURCE = SubmittedResourceIdAndSecret("filesId", "filesSecret")
  private val SUBMITTED_RESOURCES = SubmittedResources(JARS_RESOURCE, FILES_RESOURCE)
  private val BOOTSTRAPPED_POD_ANNOTATION = "bootstrapped"
  private val TRUE = "true"
  private val APP_NAME = "spark-test"
  private val APP_RESOURCE_PREFIX = "spark-prefix"
  private val APP_ID = "spark-id"
  private val CUSTOM_LABEL_KEY = "customLabel"
  private val CUSTOM_LABEL_VALUE = "customLabelValue"
  private val DEPRECATED_CUSTOM_LABEL_KEY = "deprecatedCustomLabel"
  private val DEPRECATED_CUSTOM_LABEL_VALUE = "deprecatedCustomLabelValue"
  private val ALL_EXPECTED_LABELS = Map(
      CUSTOM_LABEL_KEY -> CUSTOM_LABEL_VALUE,
      DEPRECATED_CUSTOM_LABEL_KEY -> DEPRECATED_CUSTOM_LABEL_VALUE,
      SPARK_APP_ID_LABEL -> APP_ID,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
  private val CUSTOM_ANNOTATION_KEY = "customAnnotation"
  private val CUSTOM_ANNOTATION_VALUE = "customAnnotationValue"
  private val DEPRECATED_CUSTOM_ANNOTATION_KEY = "deprecatedCustomAnnotation"
  private val DEPRECATED_CUSTOM_ANNOTATION_VALUE = "deprecatedCustomAnnotationValue"
  private val INIT_CONTAINER_SECRET_NAME = "init-container-secret"
  private val INIT_CONTAINER_SECRET_DATA = Map("secret-key" -> "secret-data")
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val PYSPARK_APP_ARGS = Array(null, "500")
  private val APP_ARGS = Array("3", "20")
  private val SPARK_JARS = Seq(
      "hdfs://localhost:9000/app/jars/jar1.jar", "file:///app/jars/jar2.jar")
  private val RESOLVED_SPARK_JARS = Seq(
      "hdfs://localhost:9000/app/jars/jar1.jar", "file:///var/data/spark-jars/jar2.jar")
  private val RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS = Seq(
      "/var/data/spark-jars/jar1.jar", "/var/data/spark-jars/jar2.jar")
  private val SPARK_FILES = Seq(
      "hdfs://localhost:9000/app/files/file1.txt", "file:///app/files/file2.txt")
  private val PYSPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.py",
    "file:///app/files/file2.py",
    "local:///app/files/file3.py",
    "http://app/files/file4.py",
    "file:///app/files/file5.py")
  private val RESOLVED_PYSPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.py",
    "/var/spark-data/spark-files/file2.py",
    "local:///app/files/file3.py",
    "http://app/files/file4.py")
  private val PYSPARK_PRIMARY_FILE = "file:///app/files/file5.py"
  private val RESOLVED_PYSPARK_PRIMARY_FILE = "/var/spark-data/spark-file/file5.py"

  private val RESOLVED_SPARK_FILES = Seq(
      "hdfs://localhost:9000/app/files/file1.txt", "file:///var/data/spark-files/file2.txt")
  private val INIT_CONTAINER_SECRET = new SecretBuilder()
    .withNewMetadata()
      .withName(INIT_CONTAINER_SECRET_NAME)
      .endMetadata()
    .withData(INIT_CONTAINER_SECRET_DATA.asJava)
    .build()
  private val CUSTOM_JAVA_OPTION_KEY = "myappoption"
  private val CUSTOM_JAVA_OPTION_VALUE = "myappoptionvalue"
  private val DRIVER_JAVA_OPTIONS = s"-D$CUSTOM_JAVA_OPTION_KEY=$CUSTOM_JAVA_OPTION_VALUE"
  private val DRIVER_EXTRA_CLASSPATH = "/var/data/spark-app-custom/custom-jar.jar"
  private val CONFIG_MAP_NAME = "config-map"
  private val CONFIG_MAP_DATA = Map("config-map-key" -> "config-map-data")
  private val INIT_CONTAINER_CONFIG_MAP = new ConfigMapBuilder()
    .withNewMetadata()
      .withName(CONFIG_MAP_NAME)
      .endMetadata()
    .withData(CONFIG_MAP_DATA.asJava)
    .build()
  private val CUSTOM_DRIVER_IMAGE = "spark-custom-driver:latest"
  private val DRIVER_MEMORY_MB = 512
  private val DRIVER_MEMORY_OVERHEAD_MB = 128
  private val SPARK_CONF = new SparkConf(true)
      .set(DRIVER_DOCKER_IMAGE, CUSTOM_DRIVER_IMAGE)
      .set(org.apache.spark.internal.config.DRIVER_MEMORY, DRIVER_MEMORY_MB.toLong)
      .set(KUBERNETES_DRIVER_MEMORY_OVERHEAD, DRIVER_MEMORY_OVERHEAD_MB.toLong)
      .set(KUBERNETES_DRIVER_LABELS, s"$DEPRECATED_CUSTOM_LABEL_KEY=$DEPRECATED_CUSTOM_LABEL_VALUE")
      .set(KUBERNETES_DRIVER_ANNOTATIONS,
          s"$DEPRECATED_CUSTOM_ANNOTATION_KEY=$DEPRECATED_CUSTOM_ANNOTATION_VALUE")
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)
      .set(s"$KUBERNETES_DRIVER_ANNOTATION_PREFIX$CUSTOM_ANNOTATION_KEY", CUSTOM_ANNOTATION_VALUE)
      .set(org.apache.spark.internal.config.DRIVER_CLASS_PATH, DRIVER_EXTRA_CLASSPATH)
      .set(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS, DRIVER_JAVA_OPTIONS)
  private val EXECUTOR_INIT_CONF_KEY = "executor-init-conf"
  private val SPARK_CONF_WITH_EXECUTOR_INIT_CONF = SPARK_CONF.clone()
      .set(EXECUTOR_INIT_CONF_KEY, TRUE)
  private val DRIVER_POD_UID = "driver-pod-uid"
  private val DRIVER_POD_KIND = "pod"
  private val DRIVER_POD_API_VERSION = "v1"
  private val CREDENTIALS_SECRET_NAME = "credentials-secret"
  private val CREDENTIALS_SECRET_DATA = Map("credentials-secret-key" -> "credentials-secret-value")
  private val CREDENTIALS_SECRET = new SecretBuilder()
    .withNewMetadata()
      .withName(CREDENTIALS_SECRET_NAME)
      .endMetadata()
    .withData(CREDENTIALS_SECRET_DATA.asJava)
    .build()
  private val CREDENTIALS_SET_CONF = "spark.kubernetes.driverCredentials.provided"
  private val CREDENTIALS_SET_ANNOTATION = "credentials-set"

  @Mock
  private var initContainerConfigMapBuilder: SparkInitContainerConfigMapBuilder = _
  @Mock
  private var containerLocalizedFilesResolver: ContainerLocalizedFilesResolver = _
  @Mock
  private var executorInitContainerConfiguration: ExecutorInitContainerConfiguration = _
  @Mock
  private var submittedDependencyUploader: SubmittedDependencyUploader = _
  @Mock
  private var submittedDependenciesSecretBuilder: SubmittedDependencySecretBuilder = _
  @Mock
  private var initContainerBootstrap: SparkPodInitContainerBootstrap = _
  @Mock
  private var initContainerComponentsProvider: DriverInitContainerComponentsProvider = _
  @Mock
  private var kubernetesClient: KubernetesClient = _
  @Mock
  private var podOps: MixedOperation[
    Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]] = _
  private type ResourceListOps = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
      HasMetadata, java.lang.Boolean]
  @Mock
  private var pythonSubmissionResources : PythonSubmissionResources = _
  @Mock
  private var resourceListOps: ResourceListOps = _
  @Mock
  private var credentialsMounterProvider: DriverPodKubernetesCredentialsMounterProvider = _
  @Mock
  private var fileMounter: DriverPodKubernetesFileMounter = _
  @Mock
  private var credentialsMounter: DriverPodKubernetesCredentialsMounter = _
  @Mock
  private var loggingPodStatusWatcher: LoggingPodStatusWatcher = _
  @Mock
  private var namedPodResource: PodResource[Pod, DoneablePod] = _
  @Mock
  private var watch: Watch = _

  before {
    MockitoAnnotations.initMocks(this)
    when(initContainerComponentsProvider.provideInitContainerBootstrap())
      .thenReturn(initContainerBootstrap)
    when(submittedDependencyUploader.uploadJars()).thenReturn(JARS_RESOURCE)
    when(submittedDependencyUploader.uploadFiles()).thenReturn(FILES_RESOURCE)
    when(initContainerBootstrap
      .bootstrapInitContainerAndVolumes(mockitoEq(DRIVER_CONTAINER_NAME), any()))
      .thenAnswer(new Answer[PodBuilder] {
        override def answer(invocationOnMock: InvocationOnMock): PodBuilder = {
          invocationOnMock.getArgumentAt(1, classOf[PodBuilder]).editMetadata()
            .addToAnnotations(BOOTSTRAPPED_POD_ANNOTATION, TRUE)
            .endMetadata()
        }
      })
    when(initContainerComponentsProvider.provideContainerLocalizedFilesResolver(any[String]))
      .thenReturn(containerLocalizedFilesResolver)
    when(initContainerComponentsProvider.provideExecutorInitContainerConfiguration())
      .thenReturn(executorInitContainerConfiguration)
    when(initContainerComponentsProvider.provideDriverPodFileMounter())
        .thenReturn(fileMounter)
    when(submittedDependenciesSecretBuilder.build())
      .thenReturn(INIT_CONTAINER_SECRET)
    when(initContainerConfigMapBuilder.build())
      .thenReturn(INIT_CONTAINER_CONFIG_MAP)
    when(kubernetesClient.pods()).thenReturn(podOps)
    when(podOps.create(any())).thenAnswer(new Answer[Pod] {
      override def answer(invocation: InvocationOnMock): Pod = {
        new PodBuilder(invocation.getArgumentAt(0, classOf[Pod]))
          .editMetadata()
            .withUid(DRIVER_POD_UID)
          .endMetadata()
            .withKind(DRIVER_POD_KIND)
          .withApiVersion(DRIVER_POD_API_VERSION)
          .build()
      }
    })
    when(podOps.withName(s"$APP_RESOURCE_PREFIX-driver")).thenReturn(namedPodResource)
    when(pythonSubmissionResources.sparkJars).thenReturn(Seq.empty[String])
    when(pythonSubmissionResources.primarySparkResource(any()))
      .thenReturn(RESOLVED_PYSPARK_PRIMARY_FILE)
    when(pythonSubmissionResources.pySparkFiles).thenReturn(PYSPARK_FILES.toArray)
    when(pythonSubmissionResources.arguments).thenReturn(Array(PYSPARK_FILES.mkString(","), "500"))
    when(pythonSubmissionResources.driverPod(
      any[DriverInitContainerComponentsProvider],
      mockitoEq(RESOLVED_PYSPARK_PRIMARY_FILE),
      mockitoEq(RESOLVED_PYSPARK_FILES.mkString(",")),
      any[String],
      any[PodBuilder])).thenAnswer( new Answer[Pod] {
        override def answer(invocation: InvocationOnMock) : Pod = {
          invocation.getArgumentAt(0, classOf[DriverInitContainerComponentsProvider])
           .provideDriverPodFileMounter().addPySparkFiles(
            invocation.getArgumentAt(1, classOf[String]),
            invocation.getArgumentAt(2, classOf[String]),
            invocation.getArgumentAt(3, classOf[String]),
            invocation.getArgumentAt(4, classOf[PodBuilder])
          ).build()
        }
      })
    when(fileMounter.addPySparkFiles(
      mockitoEq(RESOLVED_PYSPARK_PRIMARY_FILE),
      mockitoEq(RESOLVED_PYSPARK_FILES.mkString(",")),
      any[String],
      any())).thenAnswer( new Answer[PodBuilder] {
        override def answer(invocation: InvocationOnMock) : PodBuilder = {
          invocation.getArgumentAt(3, classOf[PodBuilder])
          .editSpec()
            .editMatchingContainer(new ContainerNameEqualityPredicate(
              invocation.getArgumentAt(2, classOf[String])))
              .addNewEnv()
                .withName(ENV_PYSPARK_PRIMARY)
                .withValue(invocation.getArgumentAt(0, classOf[String]))
              .endEnv()
              .addNewEnv()
                .withName(ENV_PYSPARK_FILES)
                .withValue(invocation.getArgumentAt(1, classOf[String]))
              .endEnv()
            .endContainer()
          .endSpec()
          .editMetadata()
            .withUid(DRIVER_POD_UID)
            .withName(s"$APP_RESOURCE_PREFIX-driver")
          .endMetadata()
          .withKind(DRIVER_POD_KIND)
          .withApiVersion(DRIVER_POD_API_VERSION)
        }
      })
    when(namedPodResource.watch(loggingPodStatusWatcher)).thenReturn(watch)
    when(containerLocalizedFilesResolver.resolveSubmittedAndRemoteSparkJars())
        .thenReturn(RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS)
    when(containerLocalizedFilesResolver.resolveSubmittedSparkJars())
        .thenReturn(RESOLVED_SPARK_JARS)
    when(containerLocalizedFilesResolver.resolveSubmittedSparkFiles())
        .thenReturn(RESOLVED_SPARK_FILES)
    when(containerLocalizedFilesResolver.resolvePrimaryResourceFile())
      .thenReturn(RESOLVED_PYSPARK_PRIMARY_FILE)
    when(containerLocalizedFilesResolver.resolveSubmittedPySparkFiles())
      .thenReturn(RESOLVED_PYSPARK_FILES)
    when(executorInitContainerConfiguration.configureSparkConfForExecutorInitContainer(SPARK_CONF))
        .thenReturn(SPARK_CONF_WITH_EXECUTOR_INIT_CONF)
    when(kubernetesClient.resourceList(anyVararg[HasMetadata]())).thenReturn(resourceListOps)
    when(credentialsMounterProvider.getDriverPodKubernetesCredentialsMounter())
        .thenReturn(credentialsMounter)
  }

  test("Run with dependency uploader") {
    expectationsForNoMountedCredentials()
    when(initContainerComponentsProvider
        .provideInitContainerSubmittedDependencyUploader(ALL_EXPECTED_LABELS))
        .thenReturn(Some(submittedDependencyUploader))
    when(initContainerComponentsProvider
        .provideSubmittedDependenciesSecretBuilder(Some(SUBMITTED_RESOURCES.secrets())))
        .thenReturn(Some(submittedDependenciesSecretBuilder))
    when(initContainerComponentsProvider
        .provideInitContainerConfigMapBuilder(Some(SUBMITTED_RESOURCES.ids())))
        .thenReturn(initContainerConfigMapBuilder)
    runAndVerifyDriverPodHasCorrectProperties()
    val resourceListArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    verify(kubernetesClient).resourceList(resourceListArgumentCaptor.capture())
    val createdResources = resourceListArgumentCaptor.getAllValues.asScala
    assert(createdResources.size === 2)
    verifyCreatedResourcesHaveOwnerReferences(createdResources)
    assert(createdResources.exists {
      case secret: Secret =>
        secret.getMetadata.getName == INIT_CONTAINER_SECRET_NAME &&
            secret.getData.asScala == INIT_CONTAINER_SECRET_DATA
      case _ => false
    })
    verifyConfigMapWasCreated(createdResources)
    verify(submittedDependencyUploader).uploadJars()
    verify(submittedDependencyUploader).uploadFiles()
    verify(initContainerComponentsProvider)
        .provideInitContainerConfigMapBuilder(Some(SUBMITTED_RESOURCES.ids()))
    verify(initContainerComponentsProvider)
      .provideSubmittedDependenciesSecretBuilder(Some(SUBMITTED_RESOURCES.secrets()))
  }

  test("Run without dependency uploader") {
    expectationsForNoMountedCredentials()
    expectationsForNoDependencyUploader()
    runAndVerifyDriverPodHasCorrectProperties()
    val resourceListArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    verify(kubernetesClient).resourceList(resourceListArgumentCaptor.capture())
    val createdResources = resourceListArgumentCaptor.getAllValues.asScala
    assert(createdResources.size === 1)
    verifyCreatedResourcesHaveOwnerReferences(createdResources)
    verifyConfigMapWasCreated(createdResources)
    verify(submittedDependencyUploader, times(0)).uploadJars()
    verify(submittedDependencyUploader, times(0)).uploadFiles()
    verify(initContainerComponentsProvider)
      .provideInitContainerConfigMapBuilder(None)
    verify(initContainerComponentsProvider)
      .provideSubmittedDependenciesSecretBuilder(None)
  }

  test("Run with mounted credentials") {
    expectationsForNoDependencyUploader()
    when(credentialsMounter.createCredentialsSecret()).thenReturn(Some(CREDENTIALS_SECRET))
    when(credentialsMounter.mountDriverKubernetesCredentials(
        any(), mockitoEq(DRIVER_CONTAINER_NAME), mockitoEq(Some(CREDENTIALS_SECRET))))
        .thenAnswer(new Answer[PodBuilder] {
          override def answer(invocation: InvocationOnMock): PodBuilder = {
            invocation.getArgumentAt(0, classOf[PodBuilder]).editMetadata()
              .addToAnnotations(CREDENTIALS_SET_ANNOTATION, TRUE)
              .endMetadata()
          }
        })
    when(credentialsMounter.setDriverPodKubernetesCredentialLocations(any()))
        .thenAnswer(new Answer[SparkConf] {
          override def answer(invocation: InvocationOnMock): SparkConf = {
            invocation.getArgumentAt(0, classOf[SparkConf]).clone().set(CREDENTIALS_SET_CONF, TRUE)
          }
        })
    runAndVerifyPodMatchesPredicate { p =>
      Option(p)
        .filter(pod => containerHasCorrectJvmOptions(pod, _(CREDENTIALS_SET_CONF) == TRUE))
        .exists { pod =>
          pod.getMetadata.getAnnotations.asScala(CREDENTIALS_SET_ANNOTATION) == TRUE
        }
    }
    val resourceListArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    verify(kubernetesClient).resourceList(resourceListArgumentCaptor.capture())
    val createdResources = resourceListArgumentCaptor.getAllValues.asScala
    assert(createdResources.size === 2)
    verifyCreatedResourcesHaveOwnerReferences(createdResources)
    assert(createdResources.exists {
      case secret: Secret =>
        secret.getMetadata.getName == CREDENTIALS_SECRET_NAME &&
            secret.getData.asScala == CREDENTIALS_SECRET_DATA
      case _ => false
    })
  }

  test("Waiting for completion should await completion on the status watcher.") {
    expectationsForNoMountedCredentials()
    expectationsForNoDependencyUploader()
    new Client(
      APP_NAME,
      APP_RESOURCE_PREFIX,
      APP_ID,
      "",
      None,
      MAIN_CLASS,
      SPARK_CONF,
      APP_ARGS,
      true,
      kubernetesClient,
      initContainerComponentsProvider,
      credentialsMounterProvider,
      loggingPodStatusWatcher).run()
    verify(loggingPodStatusWatcher).awaitCompletion()
  }

  test("Mounting environmental variables correctly onto Driver Pod for PySpark Jobs") {
    expectationsForNoMountedCredentials()
    expectationsForNoDependencyUploader()
    expectationsForNoSparkJarsOrFiles()
    runAndVerifyDriverPodHasCorrectPySparkProperties()
  }

  private def expectationsForNoSparkJarsOrFiles(): Unit = {
    when(containerLocalizedFilesResolver.resolveSubmittedSparkFiles())
        .thenReturn(Nil)
    when(containerLocalizedFilesResolver.resolveSubmittedSparkJars())
      .thenReturn(Nil)
  }

  private def expectationsForNoDependencyUploader(): Unit = {
    when(initContainerComponentsProvider
      .provideInitContainerSubmittedDependencyUploader(ALL_EXPECTED_LABELS))
      .thenReturn(None)
    when(initContainerComponentsProvider
      .provideSubmittedDependenciesSecretBuilder(None))
      .thenReturn(None)
    when(initContainerComponentsProvider
      .provideInitContainerConfigMapBuilder(None))
      .thenReturn(initContainerConfigMapBuilder)
  }

  private def expectationsForNoMountedCredentials(): Unit = {
    when(credentialsMounter.setDriverPodKubernetesCredentialLocations(any()))
        .thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(credentialsMounter.createCredentialsSecret()).thenReturn(None)
    when(credentialsMounter.mountDriverKubernetesCredentials(
        any(), mockitoEq(DRIVER_CONTAINER_NAME), mockitoEq(None)))
        .thenAnswer(AdditionalAnswers.returnsFirstArg())
  }

  private def verifyCreatedResourcesHaveOwnerReferences(
      createdResources: mutable.Buffer[HasMetadata]): Unit = {
    assert(createdResources.forall { resource =>
      val owners = resource.getMetadata.getOwnerReferences.asScala
      owners.size === 1 &&
        owners.head.getController &&
        owners.head.getKind == DRIVER_POD_KIND &&
        owners.head.getUid == DRIVER_POD_UID &&
        owners.head.getName == s"$APP_RESOURCE_PREFIX-driver" &&
        owners.head.getApiVersion == DRIVER_POD_API_VERSION
    })
  }

  private def verifyConfigMapWasCreated(createdResources: mutable.Buffer[HasMetadata]): Unit = {
    assert(createdResources.exists {
      case configMap: ConfigMap =>
        configMap.getMetadata.getName == CONFIG_MAP_NAME &&
            configMap.getData.asScala == CONFIG_MAP_DATA
      case _ => false
    })
  }

  private def runAndVerifyDriverPodHasCorrectProperties(): Unit = {
    val expectedOptions = SPARK_CONF.getAll
      .filterNot(_._1 == org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS.key)
      .toMap ++
      Map(
        "spark.app.id" -> APP_ID,
        KUBERNETES_DRIVER_POD_NAME.key -> s"$APP_RESOURCE_PREFIX-driver",
        KUBERNETES_EXECUTOR_POD_NAME_PREFIX.key -> APP_RESOURCE_PREFIX,
        EXECUTOR_INIT_CONF_KEY -> TRUE,
        CUSTOM_JAVA_OPTION_KEY -> CUSTOM_JAVA_OPTION_VALUE,
        "spark.jars" -> RESOLVED_SPARK_JARS.mkString(","),
        "spark.files" -> RESOLVED_SPARK_FILES.mkString(","))
    runAndVerifyPodMatchesPredicate { p =>
      Option(p)
        .filter(_.getMetadata.getName == s"$APP_RESOURCE_PREFIX-driver")
        .filter(podHasCorrectAnnotations)
        .filter(_.getMetadata.getLabels.asScala == ALL_EXPECTED_LABELS)
        .filter(containerHasCorrectBasicContainerConfiguration)
        .filter(containerHasCorrectBasicEnvs)
        .filter(containerHasCorrectMountedClasspath)
        .exists(pod => containerHasCorrectJvmOptions(pod, _ == expectedOptions))
    }
  }

  private def runAndVerifyDriverPodHasCorrectPySparkProperties(): Unit = {
    runAndVerifyPySparkPodMatchesPredicate { p =>
      Option(p).exists(pod => containerHasCorrectPySparkEnvs(pod))
    }
  }

  private def runAndVerifyPodMatchesPredicate(pred: (Pod => Boolean)): Unit = {
    new Client(
      APP_NAME,
      APP_RESOURCE_PREFIX,
      APP_ID,
      "",
      None,
      MAIN_CLASS,
      SPARK_CONF,
      APP_ARGS,
      false,
      kubernetesClient,
      initContainerComponentsProvider,
      credentialsMounterProvider,
      loggingPodStatusWatcher).run()
    val podMatcher = new BaseMatcher[Pod] {
      override def matches(o: scala.Any): Boolean = {
        o match {
          case p: Pod => pred(p)
          case _ => false
        }
      }
      override def describeTo(description: Description): Unit = {}
    }
    verify(podOps).create(argThat(podMatcher))
  }

  private def containerHasCorrectJvmOptions(
      pod: Pod, optionsCorrectnessPredicate: (Map[String, String] => Boolean)): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    envs.toMap.get(ENV_DRIVER_JAVA_OPTS).exists { javaOptions =>
      val splitOptions = javaOptions.split(" ")
      splitOptions.forall(_.startsWith("-D")) &&
        optionsCorrectnessPredicate(splitOptions.map { option =>
          val withoutPrefix = option.substring(2)
          (withoutPrefix.split("=", 2)(0), withoutPrefix.split("=", 2)(1))
        }.toMap)
    }
  }

  private def containerHasCorrectMountedClasspath(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    envs.toMap.get(ENV_MOUNTED_CLASSPATH).exists { classpath =>
      val mountedClasspathEntities = classpath.split(File.pathSeparator)
      mountedClasspathEntities.toSet == RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS.toSet
    }
  }

  private def containerHasCorrectBasicEnvs(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    val expectedBasicEnvs = Map(
      ENV_SUBMIT_EXTRA_CLASSPATH -> DRIVER_EXTRA_CLASSPATH,
      ENV_DRIVER_MEMORY -> s"${DRIVER_MEMORY_MB + DRIVER_MEMORY_OVERHEAD_MB}m",
      ENV_DRIVER_MAIN_CLASS -> MAIN_CLASS,
      ENV_DRIVER_ARGS -> APP_ARGS.mkString(" "))
    expectedBasicEnvs.toSet.subsetOf(envs.toSet)
  }

  private def containerHasCorrectPySparkEnvs(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    val expectedBasicEnvs = Map(
      ENV_PYSPARK_PRIMARY -> RESOLVED_PYSPARK_PRIMARY_FILE,
      ENV_PYSPARK_FILES -> RESOLVED_PYSPARK_FILES.mkString(",")
    )
    expectedBasicEnvs.toSet.subsetOf(envs.toSet)
  }

  private def containerHasCorrectBasicContainerConfiguration(pod: Pod): Boolean = {
    val containers = pod.getSpec.getContainers.asScala
    containers.size == 1 &&
      containers.head.getName == DRIVER_CONTAINER_NAME &&
      containers.head.getImage == CUSTOM_DRIVER_IMAGE &&
      containers.head.getImagePullPolicy == "IfNotPresent"
  }

  private def podHasCorrectAnnotations(pod: Pod): Boolean = {
    val expectedAnnotations = Map(
      DEPRECATED_CUSTOM_ANNOTATION_KEY -> DEPRECATED_CUSTOM_ANNOTATION_VALUE,
      CUSTOM_ANNOTATION_KEY -> CUSTOM_ANNOTATION_VALUE,
      SPARK_APP_NAME_ANNOTATION -> APP_NAME,
      BOOTSTRAPPED_POD_ANNOTATION -> TRUE)
    pod.getMetadata.getAnnotations.asScala == expectedAnnotations
  }

  private def runAndVerifyPySparkPodMatchesPredicate(pred: (Pod => Boolean)): Unit = {
    new Client(
      APP_NAME,
      APP_RESOURCE_PREFIX,
      APP_ID,
      PYSPARK_PRIMARY_FILE,
      Option(pythonSubmissionResources),
      MAIN_CLASS,
      SPARK_CONF,
      PYSPARK_APP_ARGS,
      false,
      kubernetesClient,
      initContainerComponentsProvider,
      credentialsMounterProvider,
      loggingPodStatusWatcher).run()
    val podMatcher = new BaseMatcher[Pod] {
      override def matches(o: scala.Any): Boolean = {
        o match {
          case p: Pod => pred(p)
          case _ => false
        }
      }
      override def describeTo(description: Description): Unit = {}
    }
    verify(podOps).create(argThat(podMatcher))
  }
}


