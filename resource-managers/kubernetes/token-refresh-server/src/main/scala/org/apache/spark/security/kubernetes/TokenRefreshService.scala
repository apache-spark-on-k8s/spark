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
package org.apache.spark.security.kubernetes

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Scheduler}
import io.fabric8.kubernetes.api.model.{ObjectMeta, Secret}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.security.kubernetes.constants._


private class TokenRefreshService(kubernetesClient: KubernetesClient, scheduler: Scheduler,
                                  ugi: UgiUtil,
                                  settings: Settings,
                                  clock: Clock) extends Actor with Logging {

  private val secretUidToTaskHandle = mutable.HashMap[String, Cancellable]()
  private val recentlyAddedSecretUids = mutable.HashSet[String]()
  private val extraCancellableByClass = mutable.HashMap[Class[_], Cancellable]()
  private val hadoopConf = new Configuration

  ugi.loginUserFromKeytab(settings.refreshServerKerberosPrincipal,
    REFRESH_SERVER_KERBEROS_KEYTAB_PATH)

  override def preStart(): Unit = {
    super.preStart()
    val duration = Duration(REFRESH_SERVER_KERBEROS_RELOGIN_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
    extraCancellableByClass.put(Relogin.getClass,
      scheduler.schedule(duration, duration, self, Relogin))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Relogin =>
      launchReloginTask()
    case StartRefresh(secret) =>
      startRefreshTask(secret)
    case StopRefresh(secret) =>
      removeRefreshTask(secret)
    case UpdateSecretsToTrack(secrets) =>
      updateSecretsToTrack(secrets)
    case renew: Renew =>
      scheduleRenewTask(renew)
  }

  override def postStop(): Unit = {
    super.postStop()
    secretUidToTaskHandle.values.map(_.cancel)
    extraCancellableByClass.values.map(_.cancel)
  }

  private def launchReloginTask() = {
    val task = new ReloginTask
    extraCancellableByClass.remove(task.getClass).foreach(_.cancel)  // Cancel in case of hanging
    extraCancellableByClass.put(task.getClass,
      scheduler.scheduleOnce(Duration(0L, TimeUnit.MILLISECONDS), task))
  }

  private def startRefreshTask(secret: Secret) = {
    recentlyAddedSecretUids.add(getSecretUid(secret.getMetadata))
    addRefreshTask(secret)
  }

  private def addRefreshTask(secret: Secret) = {
    val secretUid = getSecretUid(secret.getMetadata)
    secretUidToTaskHandle.remove(secretUid).foreach(_.cancel)  // Cancel in case of hanging
    secretUidToTaskHandle.put(secretUid, {
      val task = new StarterTask(secret, hadoopConf, self, clock)
      val cancellable = scheduler.scheduleOnce(
        Duration(STARTER_TASK_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS),
        task)
      logInfo(s"Started refresh of tokens in $secretUid of ${secret.getMetadata.getSelfLink}" +
        s" with $cancellable")
      cancellable
    })
  }

  private def removeRefreshTask(secret: Secret): Unit =
    removeRefreshTask(getSecretUid(secret.getMetadata))

  private def removeRefreshTask(secretUid: String): Unit = {
    secretUidToTaskHandle.remove(secretUid).foreach(cancellable => {
      logInfo(s"Canceling refresh of tokens in $secretUid")
      cancellable.cancel()
    })
  }

  private def updateSecretsToTrack(currentSecrets: List[Secret]): Unit = {
    val secretsByUids = currentSecrets.map(secret => (getSecretUid(secret.getMetadata), secret))
      .toMap
    val currentUids = secretsByUids.keySet
    val priorUids = secretUidToTaskHandle.keys.toSet
    val uidsToAdd = currentUids -- priorUids
    uidsToAdd.foreach(uid => addRefreshTask(secretsByUids(uid)))
    val uidsToRemove = priorUids -- currentUids -- recentlyAddedSecretUids
    uidsToRemove.foreach(uid => removeRefreshTask(uid))
    recentlyAddedSecretUids.clear()
  }

  private def scheduleRenewTask(renew: Renew) = {
    val secretUid = getSecretUid(renew.secretMeta)
    val priorTask = secretUidToTaskHandle.remove(secretUid)
    if (priorTask.nonEmpty) {
      priorTask.get.cancel()  // Cancel in case of hanging.
      val numConsecutiveErrors = renew.numConsecutiveErrors
      if (numConsecutiveErrors < RENEW_TASK_MAX_CONSECUTIVE_ERRORS) {
        val durationTillExpire = math.max(0L, renew.expireTime - clock.nowInMillis())
        val renewTime = math.max(0L, renew.expireTime - durationTillExpire / 10) // 90% mark.
        val durationTillRenew = math.max(0L, renewTime - clock.nowInMillis())
        val task = new RenewTask(renew, hadoopConf, self, kubernetesClient, clock)
        logInfo(s"Scheduling refresh of tokens in $secretUid of " +
          s"${renew.secretMeta.getSelfLink} at now + $durationTillRenew millis.")
        val cancellable = scheduler.scheduleOnce(
          Duration(durationTillRenew, TimeUnit.MILLISECONDS), task)
        secretUidToTaskHandle.put(secretUid, cancellable)
      } else {
        logWarning(s"Got too many errors for secret $secretUid of" +
          s" ${renew.secretMeta.getSelfLink}. Abandoning.")
        val maybeCancellable = secretUidToTaskHandle.remove(secretUid)
        maybeCancellable.foreach(_.cancel())
      }
    } else {
      logWarning(s"Could not find a StarterTask entry for a renew work for secret $secretUid of " +
        s" ${renew.secretMeta.getSelfLink}. Maybe the secret got deleted")
    }
  }

  private def getSecretUid(secret: ObjectMeta) = secret.getUid

  // Exposed for testing
  private[kubernetes] def numExtraCancellables() = extraCancellableByClass.size

  // Exposed for testing
  private[kubernetes] def hasExtraCancellable(key: Class[_], expected: Cancellable): Boolean = {
    val value = extraCancellableByClass.get(key)
    value.nonEmpty && expected == value.get
  }

  // Exposed for testing
  private[kubernetes] def numPendingSecretTasks() = secretUidToTaskHandle.size

  // Exposed for testing
  private[kubernetes] def hasSecretTaskCancellable(secretUid: String, expected: Cancellable)
          : Boolean = {
    val value = secretUidToTaskHandle.get(secretUid)
    value.nonEmpty && expected == value.get
  }
}

private class UgiUtil {

  def loginUserFromKeytab(kerberosPrincipal: String, kerberosKeytabPath: String): Unit =
    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabPath)

  def getLoginUser: UserGroupInformation = UserGroupInformation.getLoginUser

  def createProxyUser(user: String, realUser: UserGroupInformation): UserGroupInformation =
    UserGroupInformation.createProxyUser(user, realUser)
}

private class FileSystemUtil {

  def getFileSystem(hadoopConf: Configuration): FileSystem = FileSystem.get(hadoopConf)

  def renewToken(token: Token[_ <: TokenIdentifier], hadoopConf: Configuration): Long =
    token.renew(hadoopConf)
}

private class ReloginTask(ugi: UgiUtil) extends Runnable {

  def this() = this(new UgiUtil)

  override def run(): Unit = {
    ugi.getLoginUser.checkTGTAndReloginFromKeytab()
  }
}

private class StarterTask(secret: Secret,
                          hadoopConf: Configuration,
                          refreshService: ActorRef,
                          clock: Clock) extends Runnable with Logging {

  private var hasError = false

  override def run(): Unit = {
    val tokensToExpireTimes = readTokensFromSecret()
    val tokenKeys = tokensToExpireTimes.keys.map(
      token => token.getKind.toString + "@" + token.getService.toString).mkString(", ")
    val nextExpireTime = if (tokensToExpireTimes.nonEmpty) {
      val minExpireTime = tokensToExpireTimes.values.min
      logInfo(s"Read Hadoop tokens: $tokenKeys with $minExpireTime")
      minExpireTime
    } else {
      logWarning(s"Got an empty token list with secret ${secret.getMetadata.getUid} of" +
        s" ${secret.getMetadata.getSelfLink}")
      hasError = true
      getRetryTime
    }
    val numConsecutiveErrors = if (hasError) 1 else 0
    refreshService ! Renew(nextExpireTime, tokensToExpireTimes, secret.getMetadata,
      numConsecutiveErrors)
  }

  private def readTokensFromSecret(): Map[Token[_ <: TokenIdentifier], Long] = {
    val dataItems = secret.getData.asScala.filterKeys(
      _.startsWith(SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS)).toSeq.sorted
    val latestDataItem = if (dataItems.nonEmpty) Some(dataItems.max) else None
    latestDataItem.map {
      case (key, data) =>
        val matcher = TokenRefreshService.hadoopTokenPattern.matcher(key)
        val matches = matcher.matches()
        logInfo(s"Matching secret data $key, result $matches")
        val createTime = matcher.group(1).toLong
        val duration = matcher.group(2).toLong
        val expireTime = createTime + duration
        val credentials = new Credentials
        TokensSerializer.deserialize(credentials, Base64.decodeBase64(data))
        credentials.getAllTokens.asScala.toList.map {
          (_, expireTime)
        }
    }.toList.flatten.toMap
  }

  private def getRetryTime = clock.nowInMillis() + RENEW_TASK_RETRY_TIME_MILLIS
}

private class RenewTask(renew: Renew,
                        hadoopConf: Configuration,
                        refreshService: ActorRef,
                        kubernetesClient: KubernetesClient,
                        clock: Clock,
                        ugi: UgiUtil,
                        fsUtil: FileSystemUtil) extends Runnable with Logging {

  def this(renew: Renew, hadoopConf: Configuration, refreshService: ActorRef,
           client: KubernetesClient, clock: Clock) =
    this(renew, hadoopConf, refreshService, client, clock, ugi = new UgiUtil,
      fsUtil = new FileSystemUtil)

  private var hasError = false

  override def run(): Unit = {
    val deadline = renew.expireTime + RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS
    val nowMillis = clock.nowInMillis()
    val newExpireTimeByToken : Map[Token[_ <: TokenIdentifier], Long] =
      renew.tokensToExpireTimes.map {
        case (token, expireTime) =>
          val (maybeNewToken, maybeNewExpireTime) = refresh(token, expireTime, deadline, nowMillis)
          (maybeNewToken, maybeNewExpireTime)
      }
      .toMap
    if (newExpireTimeByToken.nonEmpty) {
      val newTokens = newExpireTimeByToken.keySet -- renew.tokensToExpireTimes.keySet
      if (newTokens.nonEmpty) {
        writeTokensToSecret(newExpireTimeByToken, nowMillis)
      }
      val nextExpireTime = newExpireTimeByToken.values.min
      logInfo(s"Renewed tokens $newExpireTimeByToken. Next expire time $nextExpireTime")
      val numConsecutiveErrors = if (hasError) renew.numConsecutiveErrors + 1 else 0
      refreshService ! Renew(nextExpireTime, newExpireTimeByToken, renew.secretMeta,
        numConsecutiveErrors)
    } else {
      logWarning(s"Got an empty token list with secret ${renew.secretMeta.getUid} of" +
        s" ${renew.secretMeta.getSelfLink}")
    }
  }

  private def refresh(token: Token[_ <: TokenIdentifier], expireTime: Long, deadline: Long,
                      nowMillis: Long) = {
    val maybeNewToken = maybeObtainNewToken(token, expireTime, nowMillis)
    val maybeNewExpireTime = maybeGetNewExpireTime(maybeNewToken, expireTime, deadline, nowMillis)
    (maybeNewToken, maybeNewExpireTime)
  }

  private def maybeObtainNewToken(token: Token[_ <: TokenIdentifier], expireTime: Long,
                                  nowMills: Long) = {
    val maybeNewToken = if (token.getKind.equals(DelegationTokenIdentifier.HDFS_DELEGATION_KIND)) {
      // The token can casted to AbstractDelegationTokenIdentifier below only if the token kind
      // is HDFS_DELEGATION_KIND, according to the YARN resource manager code. See if this can be
      // generalized beyond HDFS tokens.
      val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
      val maxDate = identifier.getMaxDate
      if (maxDate - expireTime < RENEW_TASK_REMAINING_TIME_BEFORE_NEW_TOKEN_MILLIS ||
        maxDate <= nowMills) {
        logDebug(s"Obtaining a new token with maxData $maxDate," +
          s" expireTime $expireTime, now $nowMills")
        val newToken = obtainNewToken(token, identifier)
        logInfo(s"Obtained token $newToken")
        newToken
      } else {
        token
      }
    } else {
      token
    }
    maybeNewToken
  }

  private def maybeGetNewExpireTime(token: Token[_ <: TokenIdentifier], expireTime: Long,
                                    deadline: Long,
                                    nowMillis: Long) = {
    if (expireTime <= deadline || expireTime <= nowMillis) {
      try {
        logDebug(s"Renewing token $token with current expire time $expireTime," +
          s" deadline $deadline, now $nowMillis")
        val newExpireTime = fsUtil.renewToken(token, hadoopConf)
        logDebug(s"Renewed token $token. Next expire time $newExpireTime")
        newExpireTime
      } catch {
        case t: Throwable =>
          logWarning(t.getMessage, t)
          hasError = true

          getRetryTime
      }
    } else {
      expireTime
    }
  }

  private def obtainNewToken(token: Token[_ <: TokenIdentifier],
                             identifier: AbstractDelegationTokenIdentifier) = {
    val owner = identifier.getOwner
    val realUser = identifier.getRealUser
    val user = if (realUser == null || realUser.toString.isEmpty || realUser.equals(owner)) {
      owner.toString
    } else {
      realUser.toString
    }
    val credentials = new Credentials
    val proxyUgi = ugi.createProxyUser(user, ugi.getLoginUser)
    val newToken = proxyUgi.doAs(new PrivilegedExceptionAction[Token[_ <: TokenIdentifier]] {

      override def run() : Token[_ <: TokenIdentifier] = {
        val fs = fsUtil.getFileSystem(hadoopConf)
        val tokens = fs.addDelegationTokens(ugi.getLoginUser.getUserName,
          credentials)
        tokens(0)
      }
    })
    newToken
  }

  private def writeTokensToSecret(tokensToExpireTimes: Map[Token[_ <: TokenIdentifier], Long],
                                  nowMillis: Long): Unit = {
    val durationUntilExpire = tokensToExpireTimes.values.min - nowMillis
    val key = s"$SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS$nowMillis-$durationUntilExpire"
    val value = TokensSerializer.serializeBase64(tokensToExpireTimes.keys)
    val secretMeta = renew.secretMeta
    val editor = kubernetesClient.secrets
      .inNamespace(secretMeta.getNamespace)
      .withName(secretMeta.getName)
      .edit()
    editor.addToData(key, value)
    val dataItemKeys = editor.getData.keySet().asScala.filter(
      _.startsWith(SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS)).toSeq.sorted
    // Remove data items except the latest two data items. A K8s secret can hold only up to 1 MB
    // data. We need to remove old data items. We keep the latest two items to avoid race conditions
    // where some newly launching executors may access the previous token.
    dataItemKeys.dropRight(2).foreach(editor.removeFromData)
    editor.done
    logInfo(s"Wrote new tokens $tokensToExpireTimes to a data item $key in" +
      s" secret ${secretMeta.getUid} of ${secretMeta.getSelfLink}")
  }

  private def getRetryTime = clock.nowInMillis() + RENEW_TASK_RETRY_TIME_MILLIS
}

private object TokensSerializer {

  def serializeBase64(tokens: Iterable[Token[_ <: TokenIdentifier]]): String = {
    val credentials = new Credentials()
    tokens.foreach(token => credentials.addToken(token.getService, token))
    val serialized = serializeCredentials(credentials)
    Base64.encodeBase64String(serialized)
  }

  private def serializeCredentials(credentials: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    credentials.writeTokenStorageToStream(dataStream)
    dataStream.flush()
    byteStream.toByteArray
  }

  def deserialize(credentials: Credentials, data: Array[Byte]): Unit = {
    val byteStream = new ByteArrayInputStream(data)
    val dataStream = new DataInputStream(byteStream)
    credentials.readTokenStorageStream(dataStream)
  }
}

private class Clock {

  def nowInMillis() : Long = System.currentTimeMillis()
}

private sealed trait Command
private case object Relogin extends Command
private case class UpdateSecretsToTrack(secrets: List[Secret]) extends Command
private case class StartRefresh(secret: Secret) extends Command
private case class Renew(expireTime: Long,
                         tokensToExpireTimes: Map[Token[_ <: TokenIdentifier], Long],
                         secretMeta: ObjectMeta,
                         numConsecutiveErrors: Int) extends Command
private case class StopRefresh(secret: Secret) extends Command

private object TokenRefreshService {

  val hadoopTokenPattern : Pattern = Pattern.compile(SECRET_DATA_ITEM_KEY_REGEX_HADOOP_TOKENS)

  def apply(system: ActorSystem, kubernetesClient: KubernetesClient,
            settings: Settings) : ActorRef = {
    system.actorOf(Props(classOf[TokenRefreshService], kubernetesClient, system.scheduler,
      new UgiUtil, settings, new Clock))
  }
}
