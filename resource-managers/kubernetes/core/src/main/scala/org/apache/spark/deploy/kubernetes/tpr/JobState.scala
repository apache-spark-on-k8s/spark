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

package org.apache.spark.deploy.kubernetes.tpr

private[spark] object JobState extends Enumeration {
  type JobState = Value

  /*
   * QUEUED - Spark Job has been queued to run
   * SUBMITTED - Driver Pod deployed but tasks are not yet scheduled on worker pod(s)
   * RUNNING - Task(s) have been allocated to worker pod(s) to run and Spark Job is now running
   * FINISHED - Spark Job ran and exited cleanly, i.e, worker pod(s) and driver pod were
   *            gracefully deleted
   * FAILED - Spark Job Failed due to error
   * KILLED - A user manually killed this Spark Job
   */
  val QUEUED, SUBMITTED, RUNNING, FINISHED, FAILED, KILLED = Value
}