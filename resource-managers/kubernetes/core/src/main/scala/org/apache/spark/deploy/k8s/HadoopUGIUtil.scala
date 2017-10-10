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
package org.apache.spark.deploy.k8s

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier


// Function of this class is merely for mocking reasons
private[spark] class HadoopUGIUtil{
  def getCurrentUser: UserGroupInformation = UserGroupInformation.getCurrentUser

  def getShortName: String = getCurrentUser.getShortUserName

  def isSecurityEnabled: Boolean = UserGroupInformation.isSecurityEnabled

  def loginUserFromKeytabAndReturnUGI(principal: String, keytab: String): UserGroupInformation =
    UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)

  def dfsAddDelegationToken(hadoopConf: Configuration, renewer: String, creds: Credentials)
    : Iterable[Token[_ <: TokenIdentifier]] =
    FileSystem.get(hadoopConf).addDelegationTokens(renewer, creds)

  def getCurrentTime: Long = System.currentTimeMillis()

   // Functions that should be in Core with Rebase to 2.3
  @deprecated("Moved to core in 2.2", "2.2")
  def getTokenRenewalInterval(
    renewedTokens: Iterable[Token[_ <: TokenIdentifier]],
    hadoopConf: Configuration): Option[Long] = {
      val renewIntervals = renewedTokens.filter {
        _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]}
        .flatMap { token =>
        Try {
          val newExpiration = token.renew(hadoopConf)
          val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          val interval = newExpiration - identifier.getIssueDate
          interval
        }.toOption}
      if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
  }

  @deprecated("Moved to core in 2.2", "2.2")
  def serialize(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  @deprecated("Moved to core in 2.2", "2.2")
  def deserialize(tokenBytes: Array[Byte]): Credentials = {
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(tokenBytes)))
    creds
  }
}
