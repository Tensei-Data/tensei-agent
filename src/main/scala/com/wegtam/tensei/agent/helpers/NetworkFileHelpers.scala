/*
 * Copyright (C) 2014 - 2017  Contributors as noted in the AUTHORS.md file
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.wegtam.tensei.agent.helpers

import java.io.{ BufferedInputStream, IOException }
import java.net.ProxySelector
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream

import akka.actor.ActorContext
import com.jcraft.jsch._
import com.wegtam.tensei.adt.ConnectionInformation
import org.apache.commons.net.ftp._
import org.apache.http.{ Header, HttpEntity, HttpStatus }
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.{ CookieSpecs, RequestConfig }
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{ BasicCredentialsProvider, HttpClientBuilder }
import org.apache.http.impl.conn.SystemDefaultRoutePlanner

trait NetworkFileHelpers {

  /**
    * Create a stream connection to the network file and return it.
    *
    * @param source    The connection information to the network file.
    * @param context   The actor context object.
    * @return The stream connection and the response of the connection.
    */
  def createStream(source: ConnectionInformation,
                   context: ActorContext): (Option[BufferedInputStream], Option[Any]) = {
    val httpCookiesEnabled =
      context.system.settings.config.getBoolean("tensei.agents.parser.http-cookies-enabled")
    val httpProxyEnabled =
      context.system.settings.config.getBoolean("tensei.agents.parser.http-proxy-enabled")
    val httpPortNumber =
      context.system.settings.config.getInt("tensei.agents.parser.http-port-number")
    val httpsPortNumber =
      context.system.settings.config.getInt("tensei.agents.parser.https-port-number")
    val httpHeaderContentEncoding =
      context.system.settings.config.getString("tensei.agents.parser.http-header-content-encoding")
    val httpHeaderContentEncodingValue = context.system.settings.config
      .getString("tensei.agents.parser.http-header-content-encoding-value")
    val httpConnectionTimeout = context.system.settings.config
      .getDuration("tensei.agents.parser.http-connection-timeout", TimeUnit.MILLISECONDS)
    val httpConnectionRequestTimeout = context.system.settings.config
      .getDuration("tensei.agents.parser.http-connection-request-timeout", TimeUnit.MILLISECONDS)
    val httpSocketTimeout = context.system.settings.config
      .getDuration("tensei.agents.parser.http-socket-timeout", TimeUnit.MILLISECONDS)

    val ftpConnectionTimeout = context.system.settings.config
      .getDuration("tensei.agents.parser.ftp-connection-timeout", TimeUnit.MILLISECONDS)
    val ftpPortNumber =
      context.system.settings.config.getInt("tensei.agents.parser.ftp-port-number")
    val ftpsPortNumber =
      context.system.settings.config.getInt("tensei.agents.parser.ftps-port-number")

    val sftpConnectionTimeout = context.system.settings.config
      .getDuration("tensei.agents.parser.sftp-connection-timeout", TimeUnit.MILLISECONDS)
    val sftpPortNumber =
      context.system.settings.config.getInt("tensei.agents.parser.sftp-port-number")

    // open the stream to the network resource
    source.uri.getScheme match {
      case "ftp" | "ftps" =>
        val client =
          source.uri.getScheme match {
            case "ftp"  => new FTPClient()
            case "ftps" => new FTPSClient()
          }

        try {
          val server =
            source.uri.getHost
          val port =
            if (source.uri.getPort > 0)
              source.uri.getPort
            else
              source.uri.getScheme match {
                case "ftp"  => ftpPortNumber
                case "ftps" => ftpsPortNumber
              }

          client.setDefaultPort(port)
          client.setConnectTimeout(ftpConnectionTimeout.toInt)
          client.connect(server)
          val reply = client.getReplyCode

          if (FTPReply.isPositiveCompletion(reply)) {
            val isLoggedIn =
              client.login(source.username.getOrElse(""), source.password.getOrElse(""))

            if (isLoggedIn) {
              val filename =
                source.uri.getPath
              val inputStream =
                client.retrieveFileStream(filename)
              (Option(new BufferedInputStream(inputStream)), Option(client))
            } else { // not logged in
              client.disconnect()
              (None, None)
            }
          } else { // no connection
            client.disconnect()
            (None, None)
          }
        } catch {
          case f: FTPConnectionClosedException =>
            client.disconnect()
            throw new RuntimeException(f)
          case e: IOException =>
            client.disconnect()
            throw new RuntimeException(e)
        } finally {
          if (client.isConnected)
            client.disconnect()
        }
      case "http" | "https" =>
        try {
          val httpClientBuilder: HttpClientBuilder = HttpClientBuilder.create()
          val requestBuilder: RequestConfig.Builder = RequestConfig
            .custom()
            .setConnectTimeout(httpConnectionTimeout.toInt)
            .setConnectionRequestTimeout(httpConnectionRequestTimeout.toInt)
            .setSocketTimeout(httpSocketTimeout.toInt)
          val httpClientRequest = httpClientBuilder.setDefaultRequestConfig(requestBuilder.build())

          // Cookies
          val globalConfig =
            // Cookie Policy - set global, can be overwritten on request level
            if (httpCookiesEnabled)
              RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()
            else
              RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build()
          val httpClientCookies = httpClientRequest.setDefaultRequestConfig(globalConfig)

          // Proxy
          val httpClientProxy =
            if (httpProxyEnabled) {
              // We try to use the default JRE proxy provider.
              val routePlanner = new SystemDefaultRoutePlanner(ProxySelector.getDefault)
              httpClientCookies.setRoutePlanner(routePlanner)
            } else
              httpClientCookies

          // Auth
          val closeableHttpClient =
            if (source.username.isDefined && source.password.isDefined) {
              val host = source.uri.getHost
              val port =
                if (source.uri.getPort > 0)
                  source.uri.getPort
                else if (source.uri.getScheme.equalsIgnoreCase("https"))
                  httpsPortNumber
                else
                  httpPortNumber

              val credsProvider: CredentialsProvider = new BasicCredentialsProvider()
              credsProvider.setCredentials(
                new AuthScope(host, port, AuthScope.ANY_REALM),
                new UsernamePasswordCredentials(source.username.get, source.password.get)
              )
              httpClientProxy.setDefaultCredentialsProvider(credsProvider).build()
            } else
              httpClientProxy.build()

          val httpGet  = new HttpGet(source.uri)
          val response = closeableHttpClient.execute(httpGet)
          try {
            response.getStatusLine.getStatusCode match {
              case HttpStatus.SC_OK | HttpStatus.SC_ACCEPTED =>
                var gzipped: Boolean       = false
                val headers: Array[Header] = response.getHeaders(httpHeaderContentEncoding)
                if (headers.length > 0) {
                  val encoding: Header = headers(0)
                  if (encoding.getValue.equalsIgnoreCase(httpHeaderContentEncodingValue)) {
                    gzipped = true
                  }
                }
                val entity: HttpEntity = response.getEntity
                val inputStream =
                  if (gzipped) {
                    val is: GZIPInputStream = new GZIPInputStream(entity.getContent)
                    is
                  } else
                    entity.getContent
                (Option(new BufferedInputStream(inputStream)), Option(response))
              case _ =>
                (None, None)
            }
          } finally {
            response.close()
          }
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
      case "sftp" =>
        try {
          val host = source.uri.getHost
          val port =
            if (source.uri.getPort > 0)
              source.uri.getPort
            else
              sftpPortNumber
          val user = source.username.getOrElse("")
          val filename =
            if (source.uri.getPath.startsWith("/"))
              source.uri.getPath.replaceFirst("/", "")
            else
              source.uri.getPath
          val password = source.password.getOrElse("")

          val jsch             = new JSch()
          val session: Session = jsch.getSession(user, host, port)
          session.setPassword(password)
          session.setConfig("StrictHostKeyChecking", "no")
          session.setTimeout(sftpConnectionTimeout.toInt)
          session.connect()
          if (session.isConnected) {
            val sftpChannel: ChannelSftp = session.openChannel("sftp").asInstanceOf[ChannelSftp]
            sftpChannel.connect()
            if (sftpChannel.isConnected) {
              val inputStream = sftpChannel.get(filename)
              if (inputStream != null)
                (Option(new BufferedInputStream(inputStream)), Option(session))
              else {
                if (session.isConnected)
                  session.disconnect()
                (None, None)
              }
            } else {
              if (session.isConnected)
                session.disconnect()
              (None, None)
            }
          } else
            (None, None)
        } catch {
          case jschE: JSchException =>
            throw new RuntimeException(jschE)
          case sftpE: SftpException =>
            throw new RuntimeException(sftpE)
          case ioE: IOException =>
            throw new RuntimeException(ioE)
        }
      case "smb" =>
        throw new RuntimeException("Not yet implemented!")
      case e =>
        throw new RuntimeException(s"Received uknown network file scheme: `$e`!")
    }
  }
}
