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

import com.spotify.docker.client.ProgressHandler
import com.spotify.docker.client.exceptions.DockerException
import com.spotify.docker.client.messages.ProgressMessage
import org.apache.spark.deploy.k8s.integrationtest.Logging

class LoggingBuildHandler() extends ProgressHandler with Logging {
  @throws[DockerException]
  def progress(message: ProgressMessage) {
    if (message.error != null) throw new DockerException(message.toString)
    else if (message.status == null) {
      logInfo(s"build: $message")
    }
  }
}