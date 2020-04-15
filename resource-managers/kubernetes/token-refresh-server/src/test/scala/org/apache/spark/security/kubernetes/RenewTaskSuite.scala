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

import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.fabric8.kubernetes.api.model.{DoneableSecret, Secret, SecretBuilder, SecretList}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NonNamespaceOperation, Resource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.mockito._
import org.mockito.Matchers.{any, anyString}
import org.mockito.Mockito.{doReturn, verify, when}
import org.scalatest.{BeforeAndAfter, FunSuiteLike}

class RenewTaskSuite extends TestKit(ActorSystem("test")) with FunSuiteLike with BeforeAndAfter {

  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var ugi: UgiUtil = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var fsUtil: FileSystemUtil = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var hdfs: DistributedFileSystem = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var loginUser: UserGroupInformation = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var proxyUser: UserGroupInformation = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var hadoopConf: Configuration = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var kubernetesClient: KubernetesClient = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var clock: Clock = _
  private val tokenRefreshServiceProbe = TestProbe()
  private val tokenRefreshService = tokenRefreshServiceProbe.ref
  private val createTime1 = 1500100000000L  // 2017/07/14-23:26:40
  private val duration1 = 86400000L  // one day in millis.
  private val token1ExpireTime = createTime1 + duration1
  private val maxDate1 = createTime1 + 7 * 86400000L  // 7 days
  private val token1 = buildHdfsToken(owner="john", renewer="refresh-server", realUser="john",
    password="token1-password", service="196.0.0.1:8020", createTime1, maxDate1)
  private val createTime2 = 1500200000000L   // 2017/07/16-03:13:20
  private val duration2 = 86400000L  // one day in millis.
  private val token2ExpireTime = createTime2 + duration2
  // maxDate2 below is just over the expire time. RenewTask will get a brand new token.
  private val maxDate2 = token2ExpireTime + 60 * 60 * 1000  // One hour after the expire time.
  private val token2 = buildHdfsToken(owner="john", renewer="refresh-server", realUser="john",
    password="token2-password", service="196.0.0.1:8020", createTime2, maxDate2)
  private val secret1 = new SecretBuilder()
    .withNewMetadata()
      .withUid("uid-0101")
      .withNamespace("namespace1")
      .withName("secret1")
    .endMetadata()
    .withData(Map(
      s"hadoop-tokens-$createTime1-$duration1" ->
        TokensSerializer.serializeBase64(List(token1))
    ).asJava)
    .build()
  private val secret2 = new SecretBuilder()
    .withNewMetadata()
    .withUid("uid-0202")
    .withNamespace("namespace2")
    .withName("secret2")
    .endMetadata()
    .withData(Map(
      s"hadoop-tokens-$createTime1-$duration1" ->
        TokensSerializer.serializeBase64(List(token1)),
      s"hadoop-tokens-$createTime2-$duration2" ->
        TokensSerializer.serializeBase64(List(token2))
    ).asJava)
    .build()
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretsOp: MixedOperation[Secret, SecretList, DoneableSecret,
    Resource[Secret, DoneableSecret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretsOpInNamespace: NonNamespaceOperation[Secret, SecretList, DoneableSecret,
    Resource[Secret, DoneableSecret]] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretOpWithName: Resource[Secret, DoneableSecret] = _
  @Mock(answer = Answers.RETURNS_SMART_NULLS)
  private var secretEditor: DoneableSecret = _

  before {
    MockitoAnnotations.initMocks(this)
    when(ugi.getLoginUser).thenReturn(loginUser)
    when(loginUser.getUserName).thenReturn("refresh-server")
    when(ugi.createProxyUser(anyString, Matchers.eq(loginUser))).thenReturn(proxyUser)
  }

  test("ReloginTask logins on Kerberos again") {
    val task = new ReloginTask(ugi)
    task.run()

    verify(loginUser).checkTGTAndReloginFromKeytab()
  }

  test("StarterTask reads a secret and schedules a renew task") {
    val task = new StarterTask(secret1, hadoopConf, tokenRefreshService, clock)
    task.run()

    val renewCommand = Renew(expireTime = token1ExpireTime,
      Map(token1 -> token1ExpireTime),
      secret1.getMetadata,
      numConsecutiveErrors = 0)
    tokenRefreshServiceProbe.expectMsg(renewCommand)
  }

  test("RenewTask just renews an existing token if maxDate is far way") {
    // maxDate1 of token1 is far away. So the RenewTask will only renew the existing token.
    val nowMillis = token1ExpireTime - 60 * 60 * 1000  // One hour before token2 expire time.
    when(clock.nowInMillis()).thenReturn(nowMillis)
    val newExpireTime = nowMillis + duration1
    when(fsUtil.renewToken(token1, hadoopConf)).thenReturn(newExpireTime)
    val renewCommand = Renew(expireTime = token1ExpireTime,
      Map(token1 -> token1ExpireTime),
      secret1.getMetadata,
      numConsecutiveErrors = 0)
    val task = new RenewTask(renewCommand, hadoopConf, tokenRefreshService, kubernetesClient, clock,
      ugi, fsUtil)
    task.run()

    val newRenewCommand = Renew(expireTime = newExpireTime,
      Map(token1 -> newExpireTime),
      secret1.getMetadata,
      numConsecutiveErrors = 0)
    tokenRefreshServiceProbe.expectMsg(newRenewCommand)  // Sent a new Renew command to the service.
  }

  test("RenewTask obtains a new token and write it back to the secret") {
    // maxDate2 of token2 is just over the expire time. So the RenewTask will get a brand new token.
    val nowMillis = token2ExpireTime - 60 * 60 * 1000  // One hour before token2 expire time.
    when(clock.nowInMillis()).thenReturn(nowMillis)
    val duration3 = 86400000L  // one day in millis.
    val maxDate3 = nowMillis + 7 * 86400000L  // 7 days
    val token3ExpireTime = nowMillis + duration3
    val token3 = buildHdfsToken(owner="john", renewer="refresh-server", realUser="john",
      password="token3-password", service="196.0.0.1:8020", nowMillis, maxDate3)
    doReturn(token3).when(proxyUser)
      .doAs(any(classOf[PrivilegedExceptionAction[Token[_ <: TokenIdentifier]]]))
    when(fsUtil.renewToken(token3, hadoopConf)).thenReturn(token3ExpireTime)
    when(kubernetesClient.secrets()).thenReturn(secretsOp)
    when(secretsOp.inNamespace("namespace2")).thenReturn(secretsOpInNamespace)
    when(secretsOpInNamespace.withName("secret2")).thenReturn(secretOpWithName)
    when(secretOpWithName.edit()).thenReturn(secretEditor)
    when(secretEditor.getData).thenReturn(Map(
      s"hadoop-tokens-$createTime1-$duration1" -> TokensSerializer.serializeBase64(List(token1)),
      s"hadoop-tokens-$createTime2-$duration2" -> TokensSerializer.serializeBase64(List(token2)),
      s"hadoop-tokens-$nowMillis-$duration3" -> TokensSerializer.serializeBase64(List(token3))
    ).asJava)
    when(secretEditor.removeFromData(anyString())).thenReturn(secretEditor)
    val renewCommand = Renew(expireTime = token2ExpireTime,
      Map(token2 -> token2ExpireTime),
      secret2.getMetadata,
      numConsecutiveErrors = 0)
    val task = new RenewTask(renewCommand, hadoopConf, tokenRefreshService, kubernetesClient, clock,
      ugi, fsUtil)
    task.run()

    verify(secretEditor)
      .addToData(s"hadoop-tokens-$nowMillis-$duration3",  // Added the new token to the secret.
        TokensSerializer.serializeBase64(List(token3)))
    verify(secretEditor)
      .removeFromData(s"hadoop-tokens-$createTime1-$duration1")  // Removed the oldest token.
    val newRenewCommand = Renew(expireTime = token3ExpireTime,
      Map(token3 -> token3ExpireTime),
      secret2.getMetadata,
      numConsecutiveErrors = 0)
    tokenRefreshServiceProbe.expectMsg(newRenewCommand)  // Sent a new Renew command to the service.

    val actionCaptor: ArgumentCaptor[PrivilegedExceptionAction[Token[_ <: TokenIdentifier]]] =
      ArgumentCaptor.forClass(classOf[PrivilegedExceptionAction[Token[_ <: TokenIdentifier]]])
    verify(proxyUser).doAs(actionCaptor.capture())
    val action = actionCaptor.getValue
    when(fsUtil.getFileSystem(hadoopConf))
      .thenReturn(hdfs)
    doReturn(Array(token3)).when(hdfs)
      .addDelegationTokens(Matchers.eq("refresh-server"), any(classOf[Credentials]))
    assert(action.run() == token3)
  }

  private def buildHdfsToken(owner: String, renewer: String, realUser: String, password: String,
                             service: String,
                             issueDate: Long,
                             maxDate:Long): Token[_ <: TokenIdentifier] = {
    val hdfsTokenIdentifier = new DelegationTokenIdentifier(new Text(owner), new Text(renewer),
      new Text(realUser))
    hdfsTokenIdentifier.setIssueDate(issueDate)
    hdfsTokenIdentifier.setMaxDate(maxDate)
    new Token[DelegationTokenIdentifier](hdfsTokenIdentifier.getBytes, password.getBytes,
      DelegationTokenIdentifier.HDFS_DELEGATION_KIND, new Text(service))
  }
}
