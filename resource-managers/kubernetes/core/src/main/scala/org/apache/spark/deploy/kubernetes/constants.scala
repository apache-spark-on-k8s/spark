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

package object constants {
  // Labels
  private[spark] val DRIVER_LAUNCHER_SELECTOR_LABEL = "driver-launcher-selector"
  private[spark] val SPARK_APP_NAME_LABEL = "spark-app-name"

  // Secrets
  private[spark] val DRIVER_CONTAINER_SECRETS_BASE_DIR = "/var/run/secrets/spark-submission"
  private[spark] val SUBMISSION_APP_SECRET_NAME = "spark-submission-server-secret"
  private[spark] val SUBMISSION_APP_SECRET_PREFIX = "spark-submission-server-secret"
  private[spark] val SUBMISSION_APP_SECRET_VOLUME_NAME = "spark-submission-secret-volume"
  private[spark] val SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME =
      "spark-submission-server-key-password"
  private[spark] val SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME =
    "spark-submission-server-keystore-password"
  private[spark] val SUBMISSION_SSL_KEYSTORE_SECRET_NAME = "spark-submission-server-keystore"
  private[spark] val SUBMISSION_SSL_SECRETS_PREFIX = "spark-submission-server-ssl"
  private[spark] val SUBMISSION_SSL_SECRETS_VOLUME_NAME = "spark-submission-server-ssl-secrets"

  // Default and fixed ports
  private[spark] val DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT = 7077
  private[spark] val DEFAULT_DRIVER_PORT = 7078
  private[spark] val DEFAULT_BLOCKMANAGER_PORT = 7079
  private[spark] val DEFAULT_UI_PORT = 4040
  private[spark] val UI_PORT_NAME = "spark-ui-port"
  private[spark] val DRIVER_LAUNCHER_SERVICE_PORT_NAME = "driver-launcher-port"

  // Miscellaneous
  private[spark] val DRIVER_LAUNCHER_CONTAINER_NAME = "spark-kubernetes-driver-launcher"
}
