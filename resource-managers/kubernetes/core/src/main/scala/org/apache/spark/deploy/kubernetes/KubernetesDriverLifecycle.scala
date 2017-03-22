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

import java.security.SecureRandom
import java.util.ServiceLoader

import com.google.common.io.BaseEncoding
import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, HasMetadata, HTTPGetActionBuilder, OwnerReferenceBuilder, Pod, PodBuilder, QuantityBuilder, Secret, SecretBuilder, Service, ServiceBuilder, ServicePortBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.ShutdownHookWrappers.withMandatoryOnErrorShutdownHook
import org.apache.spark.deploy.rest.kubernetes.DriverServiceManager
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Encapsulates the logic of bootstrapping all of the Kubernetes resources
 * for the Spark driver. This class handles all of the interaction with the Kubernetes
 * client, but the actual starting of the job is delegated to a lifecycle step provided
 * by the caller of this class.
 */
private[spark] class KubernetesDriverLifecycle(
    kubernetesAppName: String,
    kubernetesAppId: String,
    kubernetesClient: KubernetesClient,
    sparkConf: SparkConf,
    driverSslConfiguration: DriverSubmitSslConfiguration,
    driverSubmitter: KubernetesDriverSubmitter)
  extends Logging {

  import KubernetesDriverLifecycle._

  private val secretDirectory = s"$DRIVER_CONTAINER_SUBMISSION_SECRETS_BASE_DIR/$kubernetesAppId"
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val uiPort = sparkConf.getInt("spark.ui.port", DEFAULT_UI_PORT)
  private val driverSubmitTimeoutSecs = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TIMEOUT)
  private val driverServiceManager = serviceLoadDriverServiceManager(
    sparkConf.get(DRIVER_SERVICE_MANAGER_TYPE))
  private val serviceAccount = sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME)

  // Custom labels and annotations
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)
  private val parsedCustomLabels = parseKeyValuePairs(customLabels, KUBERNETES_DRIVER_LABELS.key,
    "labels")
  parsedCustomLabels.keys.foreach { key =>
    require(key != SPARK_APP_ID_LABEL, "Label with key" +
      s" $SPARK_APP_ID_LABEL cannot be used in" +
      " spark.kubernetes.driver.labels, as it is reserved for Spark's" +
      " internal configuration.")
  }
  private val driverKubernetesSelectors = (Map(
    SPARK_DRIVER_LABEL -> kubernetesAppId,
    SPARK_APP_ID_LABEL -> kubernetesAppId,
    SPARK_APP_NAME_LABEL -> kubernetesAppName)
    ++ parsedCustomLabels)
  private val customAnnotations = sparkConf.get(KUBERNETES_DRIVER_ANNOTATIONS)
  private val parsedCustomAnnotations = parseKeyValuePairs(
    customAnnotations,
    KUBERNETES_DRIVER_ANNOTATIONS.key,
    "annotations")

  // Submit server one-time secret
  private val submitServerSecretName = s"$SUBMISSION_APP_SECRET_PREFIX-$kubernetesAppId"
  private val submitServerSecretBase64 = {
    val submitServerSecretBytes = new Array[Byte](128)
    SECURE_RANDOM.nextBytes(submitServerSecretBytes)
    BaseEncoding.base64().encode(submitServerSecretBytes)
  }

  // Memory settings
  private val driverMemoryMb = sparkConf.get(org.apache.spark.internal.config.DRIVER_MEMORY)
  private val driverSubmitServerMemoryMb = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY)
  private val driverSubmitServerMemoryString = sparkConf.get(
    KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY.key,
    KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY.defaultValueString)
  private val driverContainerMemoryMb = driverMemoryMb + driverSubmitServerMemoryMb
  private val memoryOverheadMb = sparkConf
    .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverContainerMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverContainerMemoryMb + memoryOverheadMb

  def startKubernetesDriver(): Unit = {
    driverServiceManager.start(kubernetesClient, kubernetesAppId, sparkConf)
    try {
      // If fail: driver service manager handle error
      withMandatoryOnErrorShutdownHook(DRIVER_SERVICE_MANAGER_HANDLE_ERROR_PRIORITY) {
        val submitServerSecret = new SecretBuilder()
          .withNewMetadata()
          .withName(submitServerSecretName)
          .endMetadata()
          .withData(Map(SUBMISSION_APP_SECRET_NAME -> submitServerSecretBase64).asJava)
          .withType("Opaque")
          .build()
        // Construct the model (i.e. non-remote) for all Kubernetes resources.
        val driverPodModel = buildDriverPodModel(submitServerSecret)
        val driverServiceModelTemplate = buildDriverServiceTemplate()
        val driverServiceModel = driverServiceManager
          .customizeDriverService(driverServiceModelTemplate)
          .build()
        val allComponentModels = Seq(
          driverPodModel,
          submitServerSecret,
          driverServiceModel) ++ driverSslConfiguration.sslSecret
        Utils.tryWithResource(
            new KubernetesComponentReadiness(kubernetesClient, allComponentModels)) { readiness =>
          // Create the driver pod separately on the remote so that it can be assigned a UID. The
          // UID is required to configure all the other components we will create to tie their
          // owner reference to the driver pod.
          val remoteCreatedDriverPod = kubernetesClient.pods().create(driverPodModel)
          // If fail: delete driver pod
          withMandatoryOnErrorShutdownHook(CLEANUP_KUBERNETES_RESOURCES_PRIORITY) {
            try {
              val withOwnersKubernetesComponents = configureOwnerReferences(
                submitServerSecret,
                driverServiceModel,
                remoteCreatedDriverPod)
              kubernetesClient
                .resourceList(withOwnersKubernetesComponents: _*)
                .createOrReplace()
              // If fail: delete non-pod resources
              withMandatoryOnErrorShutdownHook(CLEANUP_KUBERNETES_RESOURCES_PRIORITY) {
                readiness.await(driverSubmitTimeoutSecs)
                Utils.tryLogNonFatalError {
                  kubernetesClient.secrets().delete(submitServerSecret)
                }
                Utils.tryLogNonFatalError {
                  kubernetesClient.secrets().delete(driverSslConfiguration.sslSecret.toSeq: _*)
                }
                val driverServiceFromServer = kubernetesClient
                  .services()
                  .withName(driverServiceModel.getMetadata.getName)
                  .get
                val driverUris = driverServiceManager
                  .getDriverServiceSubmissionServerUris(driverServiceFromServer)
                val submissionSparkConf = sparkConf.clone()
                val serviceName = driverServiceModel.getMetadata.getName
                val podName = driverPodModel.getMetadata.getName
                submissionSparkConf.set(KUBERNETES_DRIVER_POD_NAME, podName)
                submissionSparkConf.set(KUBERNETES_DRIVER_SERVICE_NAME, serviceName)
                driverSubmitter.submitApplicationToDriverServer(
                  driverUris,
                  submitServerSecretBase64,
                  submissionSparkConf,
                  driverSslConfiguration)
                val adjustedService = driverServiceManager
                  .adjustDriverServiceAfterSubmission(driverServiceFromServer)
                  .build()
                kubernetesClient.services().withName(serviceName).replace(adjustedService)
              } { _ =>
                kubernetesClient.resourceList(withOwnersKubernetesComponents: _*).delete
              }
              // end if fail
            } catch {
              case throwable: Throwable =>
                val errorMessage = new SubmissionFailureErrorMessageProvider(
                  driverSubmitTimeoutSecs,
                  kubernetesClient,
                  driverPodModel).getSubmitFailedErrorMessage
                throw new SparkException(errorMessage, throwable)
            }
          } { _ =>
            kubernetesClient.pods().delete(remoteCreatedDriverPod)
          }
          // end if fail
        }
      } { error =>
        driverServiceManager.handleSubmissionError(error)
      }
      // end if fail
    } finally {
      driverServiceManager.stop()
    }
  }

  private def configureOwnerReferences(
      submitServerSecret: Secret,
      driverServiceModel: Service,
      remoteCreatedDriverPod: Pod): Seq[HasMetadata] = {
    val driverPodOwnerRef = new OwnerReferenceBuilder()
      .withName(remoteCreatedDriverPod.getMetadata.getName)
      .withUid(remoteCreatedDriverPod.getMetadata.getUid)
      .withApiVersion(remoteCreatedDriverPod.getApiVersion)
      .withKind(remoteCreatedDriverPod.getKind)
      .withController(true)
      .build()
    val withOwnerReferencesSubmitServerSecret = new SecretBuilder(submitServerSecret)
      .editMetadata()
      .addToOwnerReferences(driverPodOwnerRef)
      .endMetadata()
      .build()
    val withOwnerReferencesSslSecrets = driverSslConfiguration.sslSecret.map(secret => {
      new SecretBuilder(secret)
        .editMetadata()
        .addToOwnerReferences(driverPodOwnerRef)
        .endMetadata()
        .build()
    })
    val withOwnerReferenceDriverService = new ServiceBuilder(driverServiceModel)
      .editMetadata()
      .addToOwnerReferences(driverPodOwnerRef)
      .endMetadata()
      .build()
    Seq(withOwnerReferenceDriverService, withOwnerReferencesSubmitServerSecret) ++
      withOwnerReferencesSslSecrets
  }

  private def buildContainerPorts(): Seq[ContainerPort] = {
    Seq((DRIVER_PORT_NAME, sparkConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT)),
      (BLOCK_MANAGER_PORT_NAME,
        sparkConf.getInt("spark.blockManager.port", DEFAULT_BLOCKMANAGER_PORT)),
      (SUBMISSION_SERVER_PORT_NAME, SUBMISSION_SERVER_PORT),
      (UI_PORT_NAME, uiPort)).map(port => new ContainerPortBuilder()
      .withName(port._1)
      .withContainerPort(port._2)
      .build())
  }

  private def buildDriverPodModel(submitServerSecret: Secret): Pod = {
    val containerPorts = buildContainerPorts()
    val probePingHttpGet = new HTTPGetActionBuilder()
      .withScheme(if (driverSslConfiguration.enabled) "HTTPS" else "HTTP")
      .withPath("/v1/submissions/ping")
      .withNewPort(SUBMISSION_SERVER_PORT_NAME)
      .build()
    val driverMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryMb}M")
      .build()
    val driverMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryWithOverhead}M")
      .build()
    new PodBuilder()
      .withNewMetadata()
        .withName(kubernetesAppId)
        .withLabels(driverKubernetesSelectors.asJava)
        .withAnnotations(parsedCustomAnnotations.asJava)
        .endMetadata()
      .withNewSpec()
        .withRestartPolicy("Never")
        .addNewVolume()
          .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
          .withNewSecret().withSecretName(submitServerSecret.getMetadata.getName).endSecret()
          .endVolume()
        .addToVolumes(driverSslConfiguration.sslPodVolume.toSeq: _*)
        .withServiceAccount(serviceAccount.getOrElse("default"))
        .addNewContainer()
          .withName(DRIVER_CONTAINER_NAME)
          .withImage(driverDockerImage)
          .withImagePullPolicy("IfNotPresent")
          .addNewVolumeMount()
            .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
            .withMountPath(secretDirectory)
            .withReadOnly(true)
            .endVolumeMount()
          .addToVolumeMounts(driverSslConfiguration.sslPodVolumeMount.toSeq: _*)
          .addNewEnv()
            .withName(ENV_SUBMISSION_SECRET_LOCATION)
            .withValue(s"$secretDirectory/$SUBMISSION_APP_SECRET_NAME")
            .endEnv()
          .addNewEnv()
            .withName(ENV_SUBMISSION_SERVER_PORT)
            .withValue(SUBMISSION_SERVER_PORT.toString)
            .endEnv()
          // Note that SPARK_DRIVER_MEMORY only affects the REST server via spark-class.
          .addNewEnv()
            .withName(ENV_DRIVER_MEMORY)
            .withValue(driverSubmitServerMemoryString)
            .endEnv()
          .addToEnv(driverSslConfiguration.sslPodEnvVars: _*)
          .withNewResources()
            .addToRequests("memory", driverMemoryQuantity)
            .addToLimits("memory", driverMemoryLimitQuantity)
            .endResources()
          .withPorts(containerPorts.asJava)
          .withNewReadinessProbe().withHttpGet(probePingHttpGet).endReadinessProbe()
          .endContainer()
        .endSpec()
      .build()
  }

  private def buildDriverServiceTemplate(): ServiceBuilder = {
    val driverSubmissionServicePort = new ServicePortBuilder()
      .withName(SUBMISSION_SERVER_PORT_NAME)
      .withPort(SUBMISSION_SERVER_PORT)
      .withNewTargetPort(SUBMISSION_SERVER_PORT)
      .build()
    new ServiceBuilder()
      .withNewMetadata()
        .withName(kubernetesAppId)
        .withLabels(driverKubernetesSelectors.asJava)
        .endMetadata()
      .withNewSpec()
        .withSelector(driverKubernetesSelectors.asJava)
        .withPorts(driverSubmissionServicePort)
        .endSpec()
  }

  private def serviceLoadDriverServiceManager(serviceManagerType: String): DriverServiceManager = {
    val driverServiceManagerLoader = ServiceLoader.load(classOf[DriverServiceManager])
    val matchingServiceManagers = driverServiceManagerLoader
      .iterator()
      .asScala
      .filter(_.getServiceManagerType == serviceManagerType)
      .toList
    require(matchingServiceManagers.nonEmpty,
      s"No driver service manager found matching type $serviceManagerType")
    require(matchingServiceManagers.size == 1, "Multiple service managers found" +
      s" matching type $serviceManagerType, got: " +
      matchingServiceManagers.map(_.getClass).mkString(","))
    matchingServiceManagers.head
  }

  private def parseKeyValuePairs(
      maybeKeyValues: Option[String],
      configKey: String,
      keyValueType: String): Map[String, String] = {
    maybeKeyValues.map(keyValues => {
      keyValues.split(",").map(_.trim).filterNot(_.isEmpty).map(keyValue => {
        keyValue.split("=", 2).toSeq match {
          case Seq(k, v) =>
            (k, v)
          case _ =>
            throw new SparkException(s"Custom $keyValueType set by $configKey must be a" +
              s" comma-separated list of key-value pairs, with format <key>=<value>." +
              s" Got value: $keyValue. All values: $keyValues")
        }
      }).toMap
    }).getOrElse(Map.empty[String, String])
  }
}

private[spark] object KubernetesDriverLifecycle {
  private val SECURE_RANDOM = new SecureRandom()
  private val DRIVER_SERVICE_MANAGER_STOP_PRIORITY = ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY
  private val DRIVER_SERVICE_MANAGER_HANDLE_ERROR_PRIORITY =
    DRIVER_SERVICE_MANAGER_STOP_PRIORITY + 1
  private val CLEANUP_KUBERNETES_RESOURCES_PRIORITY =
    DRIVER_SERVICE_MANAGER_HANDLE_ERROR_PRIORITY + 1
}
