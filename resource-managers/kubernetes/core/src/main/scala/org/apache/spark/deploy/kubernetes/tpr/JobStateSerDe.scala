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

import org.json4s.{CustomSerializer, JString}
import org.json4s.JsonAST.JNull

import org.apache.spark.deploy.kubernetes.tpr.JobState.JobState

 /**
  * JobState Serializer and Deserializer
  * */
object JobStateSerDe extends CustomSerializer[JobState](_ =>
  ({
    case JString("SUBMITTED") => JobState.SUBMITTED
    case JString("QUEUED") => JobState.QUEUED
    case JString("RUNNING") => JobState.RUNNING
    case JString("FINISHED") => JobState.FINISHED
    case JString("KILLED") => JobState.KILLED
    case JString("FAILED") => JobState.FAILED
    case JNull =>
      throw new UnsupportedOperationException("No JobState Specified")
  }, {
    case JobState.FAILED => JString("FAILED")
    case JobState.SUBMITTED => JString("SUBMITTED")
    case JobState.KILLED => JString("KILLED")
    case JobState.FINISHED => JString("FINISHED")
    case JobState.QUEUED => JString("QUEUED")
    case JobState.RUNNING => JString("RUNNING")
  })
)