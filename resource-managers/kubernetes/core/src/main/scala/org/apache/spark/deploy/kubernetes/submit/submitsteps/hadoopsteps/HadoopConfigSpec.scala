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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, Pod}

 /**
  * Represents a given configuration of the hadoop configuration logic, informing the
  * HadoopConfigBootstrapStep of how the driver should be configured. This includes:
  * <p>
  * - What Spark properties should be set on the driver's SparkConf for the executors
  * - The spec of the main container so that it can be modified to share volumes
  * - The spec of the driver pod EXCEPT for the addition of the given hadoop configs (e.g. volumes
  *   the hadoop logic needs)
  */
private[spark] case class HadoopConfigSpec(
//  additionalDriverSparkConf: Map[String, String],
  driverPod: Pod,
  driverContainer: Container)
