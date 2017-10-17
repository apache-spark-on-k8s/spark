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

package org.apache.spark.metrics.sink

import java.net.URI
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.Try

import com.codahale.metrics._
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.METRICS_NAMESPACE
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.prometheus.client.exporter.PushGatewayWithTimestamp


private[spark] class PrometheusSink(
                                     val property: Properties,
                                     val registry: MetricRegistry,
                                     securityMgr: SecurityManager)
  extends Sink with Logging {

  protected class Reporter(registry: MetricRegistry)
    extends ScheduledReporter(
      registry,
      "prometheus-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS) {
    val sparkConf: SparkConf = new SparkConf

    private[this] lazy val metricsNamespace: Option[String] =
      sparkConf.get(METRICS_NAMESPACE)
      .orElse(Option(sparkConf.getenv("METRICS_NAMESPACE")))

    private[this] lazy val sparkAppId: Option[String] =
      sparkConf.getOption("spark.app.id")
      .orElse(Option(sparkConf.getenv("SPARK_APPLICATION_ID")))

    private[this] lazy val executorId: Option[String] =
      Option(sparkConf.getenv("SPARK_EXECUTOR_ID"))
      .orElse(sparkConf.getOption("spark.executor.id"))



    override def report(
                         gauges: util.SortedMap[String, Gauge[_]],
                         counters: util.SortedMap[String, Counter],
                         histograms: util.SortedMap[String, Histogram],
                         meters: util.SortedMap[String, Meter],
                         timers: util.SortedMap[String, Timer]): Unit = {

      logInfo(s"metricsNamespace=$metricsNamespace, sparkAppId=$sparkAppId, " +
        s"executorId=$executorId")

      val role: String = (sparkAppId, executorId) match {
        case (Some(_), None) => "driver"
        case (Some(_), Some(_)) => "executor"
        case _ => "shuffle"
      }

      val job: String = role match {
        case "driver" => metricsNamespace.getOrElse(sparkAppId.get)
        case "executor" => metricsNamespace.getOrElse(sparkAppId.get)
        case _ => metricsNamespace.getOrElse("shuffle")
      }
      logInfo(s"role=$role, job=$job")

      val groupingKey: Map[String, String] = (role, executorId) match {
        case ("driver", _) => Map("role" -> role)
        case ("executor", Some(id)) => Map ("role" -> role, "number" -> id)
        case _ => Map("role" -> role)
      }


      pushGateway.pushAdd(pushRegistry, job, groupingKey.asJava,
        s"${System.currentTimeMillis}")

    }

  }

  val DEFAULT_PUSH_PERIOD: Int = 10
  val DEFAULT_PUSH_PERIOD_UNIT: TimeUnit = TimeUnit.SECONDS
  val DEFAULT_PUSHGATEWAY_ADDRESS: String = "127.0.0.1:9091"

  val KEY_PUSH_PERIOD = "period"
  val KEY_PUSH_PERIOD_UNIT = "unit"
  val KEY_PUSHGATEWAY_ADDRESS = "pushgateway-address"


  val pollPeriod: Int =
    Option(property.getProperty(KEY_PUSH_PERIOD))
      .map(_.toInt)
      .getOrElse(DEFAULT_PUSH_PERIOD)

  val pollUnit: TimeUnit =
    Option(property.getProperty(KEY_PUSH_PERIOD_UNIT))
      .map { s => TimeUnit.valueOf(s.toUpperCase) }
      .getOrElse(DEFAULT_PUSH_PERIOD_UNIT)

  val pushGatewayAddress =
    Option(property.getProperty(KEY_PUSHGATEWAY_ADDRESS))
      .getOrElse(DEFAULT_PUSHGATEWAY_ADDRESS)

  // validate pushgateway host:port
  Try(new URI(s"http://$pushGatewayAddress")).get

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  logInfo("Initializing Prometheus Sink...")
  logInfo(s"Metrics polling period -> $pollPeriod $pollUnit")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS -> $pushGatewayAddress")

  val pushRegistry: CollectorRegistry = new CollectorRegistry()
  val sparkMetricExports: DropwizardExports = new DropwizardExports(registry)
  val pushGateway: PushGatewayWithTimestamp = new PushGatewayWithTimestamp(pushGatewayAddress)

  val reporter = new Reporter(registry)

  override def start(): Unit = {
    sparkMetricExports.register(pushRegistry)

    reporter.start(pollPeriod, pollUnit)
  }

  override def stop(): Unit = {
    reporter.stop()
    pushRegistry.unregister(sparkMetricExports)
  }

  override def report(): Unit = {
    reporter.report()
  }
}
