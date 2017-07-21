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

import io.fabric8.kubernetes.api.model.ContainerBuilder

import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait KerberosConfBootstrap {
  def bootstrapMainContainerAndVolumes(originalPodWithMainContainer: PodWithMainContainer)
    : PodWithMainContainer
}

private[spark] class KerberosConfBootstrapImpl(
  delegationTokenLabelName: String) extends KerberosConfBootstrap{
  override def bootstrapMainContainerAndVolumes(
  originalPodWithMainContainer: PodWithMainContainer)
  : PodWithMainContainer = {
    val mainContainerWithMountedHadoopConf = new ContainerBuilder(
      originalPodWithMainContainer.mainContainer)
      .addNewEnv()
        .withName(ENV_KERBEROS_SECRET_LABEL)
        .withValue(delegationTokenLabelName)
      .endEnv()
      .build()
    originalPodWithMainContainer.copy(mainContainer = mainContainerWithMountedHadoopConf)
  }
}
