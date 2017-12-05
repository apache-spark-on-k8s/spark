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
package org.apache.spark.deploy.k8s.integrationtest.docker

import java.io.{File, PrintWriter}
import java.net.URI
import java.nio.file.Paths

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.spotify.docker.client.{DefaultDockerClient, DockerCertificates, LoggingBuildHandler}
import com.spotify.docker.client.DockerClient.{ListContainersParam, ListImagesParam, RemoveContainerParam}
import com.spotify.docker.client.messages.Container
import org.apache.http.client.utils.URIBuilder
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite
import org.apache.spark.internal.Logging
import org.apache.spark.util.{RedirectThread, Utils}

private[spark] class KubernetesSuiteDockerManager(
    dockerEnv: Map[String, String], dockerTag: String) extends Logging {

  private val DOCKER_BUILD_PATH = Paths.get("target", "docker")
  // Dockerfile paths must be relative to the build path.
  private val BASE_DOCKER_FILE = "dockerfiles/spark-base/Dockerfile"
  private val DRIVER_DOCKER_FILE = "dockerfiles/driver/Dockerfile"
  private val DRIVERPY_DOCKER_FILE = "dockerfiles/driver-py/Dockerfile"
  private val DRIVERR_DOCKER_FILE = "dockerfiles/driver-r/Dockerfile"
  private val EXECUTOR_DOCKER_FILE = "dockerfiles/executor/Dockerfile"
  private val EXECUTORPY_DOCKER_FILE = "dockerfiles/executor-py/Dockerfile"
  private val EXECUTORR_DOCKER_FILE = "dockerfiles/executor-r/Dockerfile"
  private val SHUFFLE_SERVICE_DOCKER_FILE = "dockerfiles/shuffle-service/Dockerfile"
  private val INIT_CONTAINER_DOCKER_FILE = "dockerfiles/init-container/Dockerfile"
  private val STAGING_SERVER_DOCKER_FILE = "dockerfiles/resource-staging-server/Dockerfile"
  private val STATIC_ASSET_SERVER_DOCKER_FILE =
      "dockerfiles/integration-test-asset-server/Dockerfile"
  private val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  private val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  private val dockerHost = dockerEnv.getOrElse("DOCKER_HOST",
      throw new IllegalStateException("DOCKER_HOST env not found."))

  private val originalDockerUri = URI.create(dockerHost)
  private val httpsDockerUri = new URIBuilder()
      .setHost(originalDockerUri.getHost)
      .setPort(originalDockerUri.getPort)
      .setScheme("https")
      .build()

  private val dockerCerts = dockerEnv.getOrElse("DOCKER_CERT_PATH",
      throw new IllegalStateException("DOCKER_CERT_PATH env not found."))

  private val dockerClient = new DefaultDockerClient.Builder()
      .uri(httpsDockerUri)
      .dockerCertificates(DockerCertificates
          .builder()
          .dockerCertPath(Paths.get(dockerCerts))
          .build().get())
      .build()

  def buildSparkDockerImages(): Unit = {
    Eventually.eventually(TIMEOUT, INTERVAL) { dockerClient.ping() }
    // Building Python distribution environment
    val pythonExec = sys.env.get("PYSPARK_DRIVER_PYTHON")
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("/usr/bin/python")
    val builder = new ProcessBuilder(
      Seq(pythonExec, "setup.py", "sdist").asJava)
    builder.directory(new File(DOCKER_BUILD_PATH.toFile, "python"))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()
    new RedirectThread(process.getInputStream, System.out, "redirect output").start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      logInfo(s"exitCode: $exitCode")
    }
    buildImage("spark-base", BASE_DOCKER_FILE)
    buildImage("spark-driver", DRIVER_DOCKER_FILE)
    buildImage("spark-driver-py", DRIVERPY_DOCKER_FILE)
    buildImage("spark-driver-r", DRIVERR_DOCKER_FILE)
    buildImage("spark-executor", EXECUTOR_DOCKER_FILE)
    buildImage("spark-executor-py", EXECUTORPY_DOCKER_FILE)
    buildImage("spark-executor-r", EXECUTORR_DOCKER_FILE)
    buildImage("spark-shuffle", SHUFFLE_SERVICE_DOCKER_FILE)
    buildImage("spark-resource-staging-server", STAGING_SERVER_DOCKER_FILE)
    buildImage("spark-init", INIT_CONTAINER_DOCKER_FILE)
    buildImage("spark-integration-test-asset-server", STATIC_ASSET_SERVER_DOCKER_FILE)
  }

  def deleteImages(): Unit = {
    removeRunningContainers()
    deleteImage("spark-driver")
    deleteImage("spark-driver-py")
    deleteImage("spark-driver-r")
    deleteImage("spark-executor")
    deleteImage("spark-executor-py")
    deleteImage("spark-executor-r")
    deleteImage("spark-shuffle")
    deleteImage("spark-resource-staging-server")
    deleteImage("spark-init")
    deleteImage("spark-integration-test-asset-server")
    deleteImage("spark-base")
  }

  private def buildImage(name: String, dockerFile: String): Unit = {
    log.info(s"Building Docker image - $name:$dockerTag")
    val dockerFileWithBaseTag = new File(DOCKER_BUILD_PATH.resolve(
        s"$dockerFile-$dockerTag").toAbsolutePath.toString)
    dockerFileWithBaseTag.deleteOnExit()
    try {
      val originalDockerFileText = Files.readLines(
          DOCKER_BUILD_PATH.resolve(dockerFile).toFile, Charsets.UTF_8).asScala
      val dockerFileTextWithProperBaseImage = originalDockerFileText.map(
          _.replace("FROM spark-base", s"FROM spark-base:$dockerTag"))
      Utils.tryWithResource(Files.newWriter(dockerFileWithBaseTag, Charsets.UTF_8)) { fileWriter =>
        Utils.tryWithResource(new PrintWriter(fileWriter)) { printWriter =>
          for (line <- dockerFileTextWithProperBaseImage) {
            // scalastyle:off println
            printWriter.println(line)
            // scalastyle:on println
          }
        }
      }
      dockerClient.build(
          DOCKER_BUILD_PATH,
          s"$name:$dockerTag",
          s"$dockerFile-$dockerTag",
          new LoggingBuildHandler())
    } finally {
      dockerFileWithBaseTag.delete()
    }
  }

  /**
   * Forces all containers running an image with the configured tag to halt and be removed.
   */
  private def removeRunningContainers(): Unit = {
    val imageIds = dockerClient.listImages(ListImagesParam.allImages())
        .asScala
        .filter(image => image.repoTags().asScala.exists(_.endsWith(s":$dockerTag")))
        .map(_.id())
        .toSet
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val runningContainersWithImageTag = stopRunningContainers(imageIds)
      require(
          runningContainersWithImageTag.isEmpty,
          s"${runningContainersWithImageTag.size} containers found still running" +
              s" with the image tag $dockerTag")
    }
    dockerClient.listContainers(ListContainersParam.allContainers())
        .asScala
        .filter(container => imageIds.contains(container.imageId()))
        .foreach(container => dockerClient.removeContainer(
            container.id(), RemoveContainerParam.forceKill(true)))
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val containersWithImageTag = dockerClient.listContainers(ListContainersParam.allContainers())
        .asScala
        .filter(container => imageIds.contains(container.imageId()))
      require(containersWithImageTag.isEmpty, s"${containersWithImageTag.size} containers still" +
        s" found with image tag $dockerTag.")
    }

  }

  private def stopRunningContainers(imageIds: Set[String]): Iterable[Container] = {
    val runningContainersWithImageTag = getRunningContainersWithImageIds(imageIds)
    if (runningContainersWithImageTag.nonEmpty) {
      log.info(s"Found ${runningContainersWithImageTag.size} containers running with" +
        s" an image with the tag $dockerTag. Attempting to remove these containers," +
        s" and then will stall for 2 seconds.")
      runningContainersWithImageTag.foreach { container =>
        dockerClient.stopContainer(container.id(), 5)
      }
    }
    runningContainersWithImageTag
  }

  private def getRunningContainersWithImageIds(imageIds: Set[String]): Iterable[Container] = {
    dockerClient
        .listContainers(
            ListContainersParam.allContainers(),
            ListContainersParam.withStatusRunning())
        .asScala
        .filter(container => imageIds.contains(container.imageId()))
  }

  private def deleteImage(name: String): Unit = {
    try {
      dockerClient.removeImage(s"$name:$dockerTag")
    } catch {
      case e: RuntimeException =>
        logWarning(s"Failed to delete image $name:$dockerTag. There may be images leaking in the" +
          s" docker environment which are now stale and unused.", e)
    }
  }
}
