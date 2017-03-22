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

import org.apache.spark.SparkException
import org.apache.spark.util.ShutdownHookManager

/**
 * Helpers for encapsulating cleanup logic in both a shutdown hook and a
 * finally block.
 */
private[spark] object ShutdownHookWrappers {

  /**
   * Run the given guardedExecution block with guarantees that the shutdown hook
   * will be executed if there is an error. If the JVM shuts down before the
   * guarded execution is complete, it is considered to be an error and the
   * shutdown hook is executed if possible. If no error occurs, the shutdown hook
   * should never run.
   */
  def withMandatoryOnErrorShutdownHook
      (priority: Int)
      (guardedExecution: => Unit)
      (shutdownHook: (Throwable => Unit)): Unit = {
    val shutdownHookRef = ShutdownHookManager.addShutdownHook(priority) { () =>
      shutdownHook.apply(new SparkException("Submission client shutting down."))
    }
    try {
      guardedExecution
    } catch {
      case e: Throwable =>
        shutdownHook.apply(e)
        throw e
    } finally {
      ShutdownHookManager.removeShutdownHook(shutdownHookRef)
    }
  }
}
