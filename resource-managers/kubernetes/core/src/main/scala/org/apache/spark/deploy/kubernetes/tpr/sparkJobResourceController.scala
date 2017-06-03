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

import java.io.IOException
import java.util.concurrent.ThreadPoolExecutor

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{blocking, Future, Promise}

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3.{HttpUrl, MediaType, OkHttpClient, Request, RequestBody, Response}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.SparkException
import org.apache.spark.deploy.kubernetes.tpr.JobState.JobState
import org.apache.spark.util.ThreadUtils

/**
 * This trait contains currently acceptable operations on the SparkJob Resource
 * CRUD
 */
private[spark] trait sparkJobResourceController {
  def createJobObject(name: String, status: Status): Unit
  def deleteJobObject(tprObjectName: String): Unit
  def getJobObject(name: String): SparkJobState
  def updateJobObject(jobResourcePatches: Seq[JobResourcePatch]): Unit
}

private[spark] case class Metadata(name: String,
    uid: Option[String] = None,
    labels: Option[Map[String, String]] = None,
    annotations: Option[Map[String, String]] = None)

private[spark] case class SparkJobState(apiVersion: String,
    kind: String,
    metadata: Metadata,
    status: Status)

private[spark] case class Status(creationTimeStamp: String,
    completionTimeStamp: String,
    sparkDriver: String,
    driverImage: String,
    executorImage: String,
    jobState: JobState,
    desiredExecutors: Int,
    currentExecutors: Int,
    driverUi: String)

private[spark] case class WatchObject(`type`: String, `object`: SparkJobState)

private[spark] case class JobResourcePatch(name: String, value: Any, fieldPath: String)

/**
 * Prereq - This assumes the kubernetes API has been extended using a TPR of name: Spark-job
 * See conf/kubernetes-custom-resource.yaml for a model
 *
 * CRUD + Watch Operations on SparkJob Resource
 *
 * This class contains all CRUD+Watch implementations performed
 * on the SparkJob Resource used to expose the state of a spark job
 * in kubernetes (visible via kubectl or through the k8s dashboard).
 */
private[spark] class sparkJobResourceControllerImpl(k8sClient: KubernetesClient)
    extends Logging with sparkJobResourceController {
  private val kubeMaster = k8sClient.getMasterUrl.toString
  private val httpClient: OkHttpClient =
    extractHttpClientFromK8sClient(k8sClient.asInstanceOf[BaseClient])
  private val namespace = k8sClient.getNamespace

  private implicit val formats: Formats = DefaultFormats + JobStateSerDe
  private implicit val ec: ThreadPoolExecutor = ThreadUtils
    .newDaemonCachedThreadPool("tpr-watcher-pool")

  /**
   * This method creates a resource of kind "SparkJob"
   * @return Unit
   * @throws IOException Due to failure from execute call
   * @throws SparkException when POST request is unsuccessful
   */
  override def createJobObject(name: String, status: Status): Unit = {
    val resourceObject =
      SparkJobState(s"$TPR_API_GROUP/$TPR_API_VERSION", TPR_KIND, Metadata(name), status)
    val payload = parse(write(resourceObject))
    val requestBody = RequestBody
      .create(MediaType.parse("application/json"), compact(render(payload)))
    val requestPathSegments = Seq(
      "apis", TPR_API_GROUP, TPR_API_VERSION, "namespaces", namespace, "sparkjobs")
    val url = generateHttpUrl(requestPathSegments)
    val request = new Request.Builder()
      .post(requestBody)
      .url(url)
      .build()

    logDebug(s"Create SparkJobResource Request: $request")
    var response: Response = null
    try {
      response = httpClient.newCall(request).execute()
    } catch {
      case x: IOException =>
        val msg =
          s"Failed to post resource $name. ${x.getMessage}. ${compact(render(payload))}"
        logError(msg)
        response.close()
        throw new SparkException(msg)
    }
    completeRequestWithExceptionIfNotSuccessful(
      "post",
      response,
      Option(Seq(name, response.toString, compact(render(payload)))))
    logDebug(s"Successfully posted resource $name: " +
      s"${pretty(render(parse(write(resourceObject))))}")
    response.body().close()
  }

  /**
   * This method deletes a resource of kind "SparkJob" with the specified name
   * @return Unit
   * @throws IOException Due to failure from execute call
   * @throws SparkException when DELETE request is unsuccessful
   */
  override def deleteJobObject(tprObjectName: String): Unit = {
    val requestPathSegments = Seq(
      "apis", TPR_API_GROUP, TPR_API_VERSION, "namespaces", namespace, "sparkjobs", tprObjectName)
    val url = generateHttpUrl(requestPathSegments)
    val request = new Request.Builder()
      .delete()
      .url(url)
      .build()

    logDebug(s"Delete Request: $request")
    var response: Response = null
    try {
      response = httpClient.newCall(request).execute()
    } catch {
      case x: IOException =>
        val msg =
          s"Failed to delete resource. ${x.getMessage}."
        logError(msg)
        response.close()
        throw new SparkException(msg)
    }
    completeRequestWithExceptionIfNotSuccessful(
      "delete",
      response,
      Option(Seq(tprObjectName, response.message(), request.toString)))

    response.body().close()
    logInfo(s"Successfully deleted resource $tprObjectName")
  }

  /**
   * This method GETS a resource of kind "SparkJob" with the given name
   * @return SparkJobState
   * @throws IOException Due to failure from execute call
   * @throws SparkException when GET request is unsuccessful
   */
  override def getJobObject(name: String): SparkJobState = {
    val requestPathSegments = Seq(
      "apis", TPR_API_GROUP, TPR_API_VERSION, "namespaces", namespace, "sparkjobs", name)
    val url = generateHttpUrl(requestPathSegments)
    val request = new Request.Builder()
      .get()
      .url(url)
      .build()

    logDebug(s"Get Request: $request")
    var response: Response = null
    try {
      response = httpClient.newCall(request).execute()
    } catch {
      case x: IOException =>
        val msg =
          s"Failed to get resource $name. ${x.getMessage}."
        logError(msg)
        response.close()
        throw new SparkException(msg)
    }
    completeRequestWithExceptionIfNotSuccessful(
      "get",
      response,
      Option(Seq(name, response.message()))
    )

    logInfo(s"Successfully retrieved resource $name")
    read[SparkJobState](response.body().string())
  }

  /**
   * This method Patches in Batch or singly a resource of kind "SparkJob" with the specified name
   * @return Unit
   * @throws IOException Due to failure from execute call
   * @throws SparkException when PATCH request is unsuccessful
   */
  override def updateJobObject(jobResourcePatches: Seq[JobResourcePatch]): Unit = {
    val payload = jobResourcePatches map { jobResourcePatch =>
        ("op" -> "replace") ~
          ("path" -> jobResourcePatch.fieldPath) ~
          ("value" -> jobResourcePatch.value.toString)
    }
    val requestBody =
      RequestBody.create(
        MediaType.parse("application/json-patch+json"),
        compact(render(payload)))
    val requestPathSegments = Seq(
      "apis", TPR_API_GROUP, TPR_API_VERSION, "namespaces",
      namespace, "sparkjobs", jobResourcePatches.head.name)
    val url = generateHttpUrl(requestPathSegments)
    val request = new Request.Builder()
      .patch(requestBody)
      .url(url)
      .build()

    logDebug(s"Update Request: $request")
    var response: Response = null
    try {
      response = httpClient.newCall(request).execute()
    } catch {
      case x: IOException =>
        val msg =
          s"Failed to get resource ${jobResourcePatches.head.name}. ${x.getMessage}."
        logError(msg)
        response.close()
        throw new SparkException(msg)
    }
    completeRequestWithExceptionIfNotSuccessful(
      "patch",
      response,
      Option(Seq(jobResourcePatches.head.name, response.message(), compact(render(payload))))
    )

    response.body().close()
    logDebug(s"Successfully patched resource ${jobResourcePatches.head.name}.")
  }

  private def generateHttpUrl(urlSegments: Seq[String],
    querySegments: Seq[(String, String)] = Seq.empty[(String, String)]): HttpUrl = {

    val urlBuilder = HttpUrl.parse(kubeMaster).newBuilder
    urlSegments map { pathSegment =>
      urlBuilder.addPathSegment(pathSegment)
    }
    querySegments map {
      case (query, value) => urlBuilder.addQueryParameter(query, value)
    }
    urlBuilder.build()
  }

  private def completeRequestWithExceptionIfNotSuccessful(
    requestType: String,
    response: Response,
    additionalInfo: Option[Seq[String]] = None): Unit = {

    if (!response.isSuccessful) {
      response.body().close()
      val msg = new ArrayBuffer[String]
      msg += s"Failed to $requestType resource."

      additionalInfo match {
        case Some(info) =>
          for (extraMsg <- info) {
            msg += extraMsg
          }
        case None =>
      }

      val finalMessage = msg.mkString(" ")
      logError(finalMessage)
      throw new SparkException(finalMessage)
    }
  }

  def extractHttpClientFromK8sClient(client: BaseClient): OkHttpClient = {
    val field = classOf[BaseClient].getDeclaredField("httpClient")
    try {
      field.setAccessible(true)
      field.get(client).asInstanceOf[OkHttpClient]
    } finally {
      field.setAccessible(false)
    }
  }

  private def executeBlocking(cb: => WatchObject): Future[WatchObject] = {
    val p = Promise[WatchObject]()
    ec.execute(new Runnable {
      override def run(): Unit = {
        try {
          p.trySuccess(blocking(cb))
        } catch {
          case e: Throwable => p.tryFailure(e)
        }
      }
    })
    p.future
  }

}
