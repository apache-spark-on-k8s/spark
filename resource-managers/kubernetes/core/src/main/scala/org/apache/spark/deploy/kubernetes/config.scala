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

import java.util.concurrent.TimeUnit

import org.apache.spark.{SPARK_VERSION => sparkVersion}
import org.apache.spark.internal.config.ConfigBuilder

package object config {

  private[spark] val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("""
          | The namespace that will be used for running the driver and
          | executor pods. When using spark-submit in cluster mode,
          | this can also be passed to spark-submit via the
          | --kubernetes-namespace command line argument.
        """.stripMargin)
      .stringConf
      .createWithDefault("default")

  private[spark] val DRIVER_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.docker.image")
      .doc("""
          | Docker image to use for the driver. Specify this using the
          | standard Docker tag format.
        """.stripMargin)
      .stringConf
      .createWithDefault(s"spark-driver:$sparkVersion")

  private[spark] val EXECUTOR_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.docker.image")
      .doc("""
          | Docker image to use for the executors. Specify this using
          | the standard Docker tag format.
        """.stripMargin)
      .stringConf
      .createWithDefault(s"spark-executor:$sparkVersion")

  private[spark] val KUBERNETES_CA_CERT_FILE =
    ConfigBuilder("spark.kubernetes.submit.caCertFile")
      .doc("""
          | CA cert file for connecting to Kubernetes over SSL. This
          | file should be located on the submitting machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_CLIENT_KEY_FILE =
    ConfigBuilder("spark.kubernetes.submit.clientKeyFile")
      .doc("""
          | Client key file for authenticating against the Kubernetes
          | API server. This file should be located on the submitting
          | machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_CLIENT_CERT_FILE =
    ConfigBuilder("spark.kubernetes.submit.clientCertFile")
      .doc("""
          | Client cert file for authenticating against the
          | Kubernetes API server. This file should be located on
          | the submitting machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder("spark.kubernetes.submit.serviceAccountName")
      .doc("""
          | Service account that is used when running the driver pod.
          | The driver pod uses this service account when requesting
          | executor pods from the API server.
        """.stripMargin)
      .stringConf
      .createWithDefault("default")

  private[spark] val KUBERNETES_DRIVER_UPLOAD_JARS =
    ConfigBuilder("spark.kubernetes.driver.uploads.jars")
      .doc("""
          | Comma-separated list of jars to sent to the driver and
          | all executors when submitting the application in cluster
          | mode.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_EXECUTOR_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.executor.memoryOverhead")
      .doc("""
          | The amount of off-heap memory (in megabytes) to be
          | allocated per executor. This is memory that accounts for
          | things like VM overheads, interned strings, other native
          | overheads, etc. This tends to grow with the executor size
          | (typically 6-10%).
        """.stripMargin)

  private[spark] val KUBERNETES_DRIVER_LABELS =
    ConfigBuilder("spark.kubernetes.driver.labels")
      .doc("""
          | Custom labels that will be added to the driver pod.
          | This should be a comma-separated list of label key-value
          | pairs, where each label is in the format key=value.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_LAUNCH_TIMEOUT =
    ConfigBuilder("spark.kubernetes.driverLaunchTimeout")
      .doc("""
          | Time to wait for the driver pod to be initially ready
          | before aborting the job.
        """.stripMargin)
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60L)

  private[spark] val KUBERNETES_DRIVER_LAUNCH_KEYSTORE =
    ConfigBuilder("spark.ssl.kubernetes.driverlaunch.keyStore")
      .doc("""
          | KeyStore file for the driver launch server listening
          | on SSL. Can be pre-mounted on the driver container
          | or uploaded from the submitting client.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_LAUNCH_TRUSTSTORE =
    ConfigBuilder("spark.ssl.kubernetes.driverlaunch.trustStore")
      .doc("""
          | TrustStore containing certificates for communicating
          | to the driver launch server over SSL.
        """.stripMargin)
      .stringConf
      .createOptional
}
