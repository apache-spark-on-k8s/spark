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

import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody, Response}
import okio.{Buffer, BufferedSource}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}
import org.json4s.jackson.Serialization.{read, write}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{blocking, Future, Promise}
import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.deploy.kubernetes.ClientBuilder
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.SparkException
import org.apache.spark.util.ThreadUtils

/**
 * Isolated this since the method is called on the client machine
 */

private[spark] case class Metadata(name: String,
                                   uid: Option[String] = None,
                                   labels: Option[Map[String, String]] = None,
                                   annotations: Option[Map[String, String]] = None)

private[spark] case class SparkJobState(apiVersion: String,
                                        kind: String,
                                        metadata: Metadata,
                                        spec: Map[String, Any])

private[spark] case class WatchObject(`type`: String, `object`: SparkJobState)

private[spark] abstract class TPRCrudCalls extends Logging {
  protected val k8sClient: KubernetesClient
  protected val kubeMaster: String = KUBERNETES_MASTER_INTERNAL_URL
  protected val kubeToken: Option[String] = None

  implicit val formats: Formats = DefaultFormats + JobStateSerDe


  protected val httpClient: OkHttpClient = ClientBuilder
    .buildOkhttpClientFromWithinPod(k8sClient.asInstanceOf[BaseClient])

  protected val namespace: String = k8sClient.getNamespace
  private var watchSource: BufferedSource = _
  private lazy val buffer = new Buffer()
  protected implicit val ec: ThreadPoolExecutor = ThreadUtils
    .newDaemonCachedThreadPool("tpr-watcher-pool")

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

  private def completeRequest(partialReq: Request.Builder): Request = {
    kubeToken match {
      case Some(tok) => partialReq.addHeader("Authorization", s"Bearer $tok").build()
      case None => partialReq.build()
    }
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

  def createJobObject(name: String, keyValuePairs: Map[String, Any]): Unit = {
    val resourceObject =
      SparkJobState(TPR_API_VERSION, TPR_KIND, Metadata(name), keyValuePairs)
    val payload = parse(write(resourceObject))

    val requestBody = RequestBody
      .create(MediaType.parse("application/json"), compact(render(payload)))
    val request = completeRequest(new Request.Builder()
      .post(requestBody)
      .url(s"$kubeMaster/${TPR_API_ENDPOINT.format(TPR_API_VERSION, namespace)}"))

    logDebug(s"Create Request: $request")
    val response = httpClient.newCall(request).execute()
    completeRequestWithExceptionIfNotSuccessful(
      "post",
      response,
      Option(Seq(name, response.toString, compact(render(payload)))))

    response.body().close()
    logDebug(s"Successfully posted resource $name: " +
      s"${pretty(render(parse(write(resourceObject))))}")
  }

  def deleteJobObject(tprObjectName: String): Unit = {
    val request = completeRequest(new Request.Builder()
      .delete()
      .url(s"$kubeMaster/${TPR_API_ENDPOINT.format(TPR_API_VERSION, namespace)}/$tprObjectName"))

    logDebug(s"Delete Request: $request")
    val response = httpClient.newCall(request).execute()
    completeRequestWithExceptionIfNotSuccessful(
      "delete",
      response,
      Option(Seq(tprObjectName, response.message(), request.toString)))

    response.body().close()
    logInfo(s"Successfully deleted resource $tprObjectName")
  }

  def getJobObject(name: String): SparkJobState = {
    val request = completeRequest(new Request.Builder()
      .get()
      .url(s"$kubeMaster/${TPR_API_ENDPOINT.format(TPR_API_VERSION, namespace)}/$name"))

    logDebug(s"Get Request: $request")
    val response = httpClient.newCall(request).execute()
    completeRequestWithExceptionIfNotSuccessful(
      "get",
      response,
      Option(Seq(name, response.message()))
    )

    logInfo(s"Successfully retrieved resource $name")
    read[SparkJobState](response.body().string())
  }

  def updateJobObject(name: String, value: String, fieldPath: String): Unit = {
    val payload = List(
      ("op" -> "replace") ~ ("path" -> fieldPath) ~ ("value" -> value))
    val requestBody =
      RequestBody.create(
        MediaType.parse("application/json-patch+json"),
        compact(render(payload)))
    val request = completeRequest(new Request.Builder()
      .patch(requestBody)
      .url(s"$kubeMaster/${TPR_API_ENDPOINT.format(TPR_API_VERSION, namespace)}/$name"))

    logDebug(s"Update Request: $request")
    val response = httpClient.newCall(request).execute()
    completeRequestWithExceptionIfNotSuccessful(
      "patch",
      response,
      Option(Seq(name, response.message(), compact(render(payload))))
    )

    response.body().close()
    logDebug(s"Successfully patched resource $name.")
  }

  /**
    * This method has an helper method that blocks to watch the object.
    * The future is completed on a Delete event or source exhaustion.
    * This method relies on the assumption of one sparkjob per namespace
    */
  def watchJobObject(): Future[WatchObject] = {
    val watchClient = httpClient.newBuilder().readTimeout(0, TimeUnit.MILLISECONDS).build()
    val request = completeRequest(new Request.Builder()
      .get()
      .url(s"$kubeMaster/${TPR_API_ENDPOINT.format(TPR_API_VERSION, namespace)}?watch=true"))

    logDebug(s"Watch Request: $request")
    val resp = watchClient.newCall(request).execute()
    completeRequestWithExceptionIfNotSuccessful(
      "start watch on",
      resp,
      Option(Seq(resp.code().toString, resp.message())))

    logInfo(s"Starting watch on jobResource")
    watchJobObjectUtil(resp)
  }

  /**
    * This method has a blocking call - wait on SSE - inside it.
    * However it is sent off in a new thread
    */
  private def watchJobObjectUtil(response: Response): Future[WatchObject] = {
    @volatile var wo: WatchObject = null
    watchSource = response.body().source()
    executeBlocking {
      breakable {
        // This will block until there are bytes to read or the source is exhausted.
        while (!watchSource.exhausted()) {
          watchSource.read(buffer, 8192) match {
            case -1 =>
              cleanUpListener(watchSource, buffer)
              throw new SparkException("Source is exhausted and object state is unknown")
            case _ =>
              wo = read[WatchObject](buffer.readUtf8())
              wo match {
                case WatchObject("DELETED", w) =>
                  logInfo(s"${w.metadata.name} has been deleted")
                  cleanUpListener(watchSource, buffer)
                case WatchObject(e, _) => logInfo(s"$e event. Still watching")
              }
          }
        }
      }
      wo
    }
  }

  private def cleanUpListener(source: BufferedSource, buffer: Buffer): Unit = {
    buffer.close()
    source.close()
    break()
  }

  // Serves as a way to interrupt to the watcher thread.
  // This closes the source the watcher is reading from and as a result triggers promise completion
  def stopWatcher(): Unit = {
    if (watchSource != null) {
      buffer.close()
      watchSource.close()
    }
  }

}
