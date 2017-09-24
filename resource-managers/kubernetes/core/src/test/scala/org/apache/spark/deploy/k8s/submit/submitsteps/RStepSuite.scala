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
package org.apache.spark.deploy.k8s.submit.submitsteps

import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}

class RStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val FILE_DOWNLOAD_PATH = "/var/data/spark-files"
  private val R_PRIMARY_FILE_OP1 = "local:///app/files/file1.R"
  private val RESOLVED_R_PRIMARY_FILE_OP1 = "/app/files/file1.R"
  private val R_PRIMARY_FILE_OP2 = "file:///app/files/file2.R"
  private val RESOLVED_R_PRIMARY_FILE_OP2 = FILE_DOWNLOAD_PATH + "/file2.R"

  test("testing RSpark with local file") {
    val rStep = new RStep(
      R_PRIMARY_FILE_OP1,
      FILE_DOWNLOAD_PATH)
    val returnedDriverContainer = rStep.configureDriver(
      KubernetesDriverSpec(
        new Pod(),
        new Container(),
        Seq.empty[HasMetadata],
        new SparkConf))
    assert(returnedDriverContainer.driverContainer.getEnv
      .asScala.map(env => (env.getName, env.getValue)).toMap ===
      Map(
        "R_FILE" -> RESOLVED_R_PRIMARY_FILE_OP1))
  }

  test("testing RSpark with remote file") {
    val rStep = new RStep(
      R_PRIMARY_FILE_OP2,
      FILE_DOWNLOAD_PATH)
    val returnedDriverContainer = rStep.configureDriver(
      KubernetesDriverSpec(
        new Pod(),
        new Container(),
        Seq.empty[HasMetadata],
        new SparkConf))
    assert(returnedDriverContainer.driverContainer.getEnv
      .asScala.map(env => (env.getName, env.getValue)).toMap ===
      Map(
        "R_FILE" -> RESOLVED_R_PRIMARY_FILE_OP2))
  }

}
