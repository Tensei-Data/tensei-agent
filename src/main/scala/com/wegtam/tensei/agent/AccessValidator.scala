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

package com.wegtam.tensei.agent

import java.io.{ ByteArrayInputStream, File, IOException }
import java.net.ProxySelector
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.jcraft.jsch._
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.AccessValidator.AccessValidatorMessages
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers._
import org.apache.commons.net.ftp.{ FTPClient, FTPConnectionClosedException, FTPReply, FTPSClient }
import org.apache.http.HttpStatus
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.{ CookieSpecs, RequestConfig }
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{ BasicCredentialsProvider, HttpClientBuilder }
import org.apache.http.impl.conn.SystemDefaultRoutePlanner

import scala.util.{ Random, Try }
import scalaz.Scalaz._
import scalaz._

object AccessValidator {

  /**
    * Helper method to create the access validator actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(new AccessValidator(agentRunIdentifier))

  sealed trait AccessValidatorMessages

  object AccessValidatorMessages {

    /**
      * Validate access for the given connection.
      *
      * @param con       The connection information containing the details.
      * @param writeable A flag that indicates that the connection should support write access.
      */
    case class ValidateConnection(con: ConnectionInformation, writeable: Boolean = false)

    /**
      * The result for a single connection validation.
      *
      * @param reference A reference to the connection validated. This returns the connection information that was validated.
      * @param result    A validation holding either the error messages or the connection.
      */
    case class ValidateConnectionResult(reference: ConnectionInformation,
                                        result: ValidationNel[String, ConnectionInformation])

    /**
      * Validate a bunch of connections.
      *
      * @param sources A list of source connection informations.
      * @param targets A list of target connection informations. For these write access will be checked.
      */
    case class ValidateTransformationConnections(sources: List[ConnectionInformation],
                                                 targets: List[ConnectionInformation])

    /**
      * The results for the validation of a bunch of connection informations.
      *
      * @param results A list holding the validations which in turn hold either the error messages or the successfully validated connection.
      */
    case class ValidateTransformationConnectionsResults(
        results: List[ValidationNel[String, ConnectionInformation]]
    )
  }
}

/**
  * Validates the access for a given connection.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class AccessValidator(agentRunIdentifier: Option[String])
    extends Actor
    with ActorLogging
    with DatabaseHelpers
    with NetworkFileWriterHelper {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val httpCookiesEnabled: Boolean =
    context.system.settings.config.getBoolean("tensei.agents.parser.http-cookies-enabled")
  val httpProxyEnabled: Boolean =
    context.system.settings.config.getBoolean("tensei.agents.parser.http-proxy-enabled")
  val httpPortNumber: Int =
    context.system.settings.config.getInt("tensei.agents.parser.http-port-number")
  val httpsPortNumber: Int =
    context.system.settings.config.getInt("tensei.agents.parser.https-port-number")
  val httpHeaderContentEncoding: String =
    context.system.settings.config.getString("tensei.agents.parser.http-header-content-encoding")
  val httpHeaderContentEncodingValue: String = context.system.settings.config
    .getString("tensei.agents.parser.http-header-content-encoding-value")
  val httpConnectionTimeout: Long = context.system.settings.config
    .getDuration("tensei.agents.parser.http-connection-timeout", TimeUnit.MILLISECONDS)
  val httpConnectionRequestTimeout: Long = context.system.settings.config
    .getDuration("tensei.agents.parser.http-connection-request-timeout", TimeUnit.MILLISECONDS)
  val httpSocketTimeout: Long = context.system.settings.config
    .getDuration("tensei.agents.parser.http-socket-timeout", TimeUnit.MILLISECONDS)

  val ftpPortNumber: Int =
    context.system.settings.config.getInt("tensei.agents.parser.ftp-port-number")
  val ftpsPortNumber: Int =
    context.system.settings.config.getInt("tensei.agents.parser.ftps-port-number")
  val ftpConnectionTimeout: Long = context.system.settings.config
    .getDuration("tensei.agents.parser.ftp-connection-timeout", TimeUnit.MILLISECONDS)
  val sftpPortNumber: Int =
    context.system.settings.config.getInt("tensei.agents.parser.sftp-port-number")
  val sftpConnectionTimeout: Long = context.system.settings.config
    .getDuration("tensei.agents.parser.sftp-connection-timeout", TimeUnit.MILLISECONDS)

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  def receive: PartialFunction[Any, Unit] = {
    case AccessValidatorMessages.ValidateConnection(con, writeAccess) =>
      log.debug("Starting access validation...")
      val result = checkAccess(con, writeAccess)
      sender() ! AccessValidatorMessages.ValidateConnectionResult(reference = con, result = result)
    case AccessValidatorMessages.ValidateTransformationConnections(sources, targets) =>
      log.debug("Starting access validation for {} source and {} target connections.",
                sources.size,
                targets.size)
      val sourceResults = sources.map(checkAccess(_))
      val targetResults = targets.map(checkAccess(_, writeable = true))
      sender() ! AccessValidatorMessages.ValidateTransformationConnectionsResults(
        results = sourceResults ::: targetResults
      )
  }

  /**
    * Checks whether the described connection is accessible.
    * If any of the called methods throws an exception then that is catched and wrapped into a failure.
    *
    * @param con Information about the connection.
    * @param writeable Set to `true` to check for write access.
    * @return Returns `true` on success and `false` otherwise.
    */
  private def checkAccess(
      con: ConnectionInformation,
      writeable: Boolean = false
  ): ValidationNel[String, ConnectionInformation] =
    try {
      URIHelpers.connectionType(con.uri) match {
        case ConnectionTypeFile => checkAccessFile(con, writeable)
        case ConnectionTypeFileFromNetwork =>
          checkAccessFileFromNetwork(con, writeable)
        case ConnectionTypeDatabase => checkAccessDatabase(con, writeable)
        case ConnectionTypeAPI      => checkAccessAPI(con, writeable)
        case ConnectionTypeStream   => checkAccessStream(con, writeable)
      }
    } catch {
      case e: Throwable => GenericHelpers.createValidationFromException[ConnectionInformation](e)
    }

  /**
    * Checks whether the file is accessible.
    *
    * @param con Information about the connection.
    * @param writeable Set to `true` to check for write access.
    * @return A validation holding either the error messages in a failure or the connection information in a success.
    */
  private def checkAccessFile(con: ConnectionInformation,
                              writeable: Boolean): ValidationNel[String, ConnectionInformation] = {
    val filename = con.uri.getSchemeSpecificPart
    val file     = new File(filename)

    if (writeable) {
      checkAccessFileCanWrite(file) match {
        case \/-(_) => con.successNel
        case -\/(writeError) =>
          checkAccessFileCanCreate(file) match {
            case \/-(_) => con.successNel
            case -\/(createError) =>
              (writeError.failNel[List[ConnectionInformation]] |@| createError
                .failNel[List[ConnectionInformation]]) {
                case (a, b) => con
              }
          }
      }
    } else {
      checkAccessFileExists(file) match {
        case \/-(_) =>
          checkAccessFileCanRead(file) match {
            case \/-(_)         => con.successNel
            case -\/(readError) => readError.failNel
          }
        case -\/(existsError) => existsError.failNel
      }
    }
  }

  /**
    * Chef if we can create a file.
    * If the file already exists then we consider this an error eg. a failure!
    *
    * @param file A file.
    * @return Either an error message or the file.
    */
  private def checkAccessFileCanCreate(file: File): String \/ File =
    if (!file.exists() && file.createNewFile())
      file.right
    else
      s"Cannot create new file: ${file.getAbsolutePath}!".left

  /**
    * Check if we can read the given file.
    * Please not that the file has to exist and that the `canRead` function
    * of the `File` class may return `true` although the file has no read
    * permissions.
    *
    * @param file A file.
    * @return Either an error message or the file.
    */
  private def checkAccessFileCanRead(file: File): String \/ File =
    if (file.canRead)
      file.right
    else
      s"Cannot read file: ${file.getAbsolutePath}!".left

  /**
    * Check if we are able to write into the given file.
    * If the file does not exist then we consider this an error!
    *
    * @param file A file.
    * @return Either an error message or the file.
    */
  private def checkAccessFileCanWrite(file: File): String \/ File =
    if (file.exists() && file.canWrite)
      file.right
    else
      s"Cannot write to file: ${file.getAbsolutePath}!".left

  /**
    * Check if the given file exists.
    *
    * @param file A file.
    * @return Either an error message or the file.
    */
  private def checkAccessFileExists(file: File): String \/ File =
    if (file.exists())
      file.right
    else
      s"File ${file.getAbsolutePath} does not exist!".left

  /**
    * Checks whether the network file is accessible.
    *
    * @param con Information about the connection.
    * @param writeable Set to `true` to check for write access.
    * @return A validation holding either the error messages in a failure or the connection information in a success.
    */
  private def checkAccessFileFromNetwork(
      con: ConnectionInformation,
      writeable: Boolean
  ): ValidationNel[String, ConnectionInformation] =
    if (writeable) {
      con.uri.getScheme match {
        case "ftp" =>
          testWriteableFtpFile(con)

        case "ftps" =>
          testWriteableFtpsFile(con)

        case "sftp" =>
          s"Writeable not yet implemented for network files with scheme `${con.uri.getScheme}`!".failNel

        case _ =>
          s"Writeable not yet implemented for network files with scheme `${con.uri.getScheme}`!".failNel
      }
    } else {
      // open the stream to the network resource
      con.uri.getScheme match {
        case "ftp" =>
          "FTP not yet implemented for network files!".failNel
        case "http" =>
          try {
            val httpClientBuilder: HttpClientBuilder = HttpClientBuilder.create()
            val requestBuilder: RequestConfig.Builder = RequestConfig
              .custom()
              .setConnectTimeout(httpConnectionTimeout.toInt)
              .setConnectionRequestTimeout(httpConnectionRequestTimeout.toInt)
              .setSocketTimeout(httpSocketTimeout.toInt)
            val httpClientRequest =
              httpClientBuilder.setDefaultRequestConfig(requestBuilder.build())

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
              if (con.username.isDefined && con.password.isDefined) {
                val host = con.uri.getHost
                val port =
                  if (con.uri.getScheme.equalsIgnoreCase("https"))
                    httpsPortNumber
                  else
                    httpPortNumber

                require(con.username.isDefined, "No username set in credentials!")
                require(con.password.isDefined, "No password set in credentials!")
                val credsProvider: CredentialsProvider = new BasicCredentialsProvider()
                credsProvider.setCredentials(
                  new AuthScope(host, port, AuthScope.ANY_REALM),
                  new UsernamePasswordCredentials(con.username.getOrElse(""),
                                                  con.password.getOrElse(""))
                )
                httpClientProxy.setDefaultCredentialsProvider(credsProvider).build()
              } else
                httpClientProxy.build()

            val httpGet  = new HttpGet(con.uri)
            val response = closeableHttpClient.execute(httpGet)
            try {
              response.getStatusLine.getStatusCode match {
                case HttpStatus.SC_OK | HttpStatus.SC_ACCEPTED =>
                  con.successNel
                case _ =>
                  s"Error during HTTP connection for network file `${con.uri}`: ${response.getStatusLine}".failNel
              }
            } finally {
              response.close()
            }
          } catch {
            case e: IOException =>
              GenericHelpers.createValidationFromException[ConnectionInformation](e)
          }
        case "smb" =>
          "Not yet implemented!".failNel
        case e =>
          s"Received uknown network file scheme: `$e`!".failNel
      }
    }

  @throws[FTPConnectionClosedException]
  @throws[IOException]
  private def testWriteableFtpFile(
      con: ConnectionInformation
  ): ValidationNel[String, ConnectionInformation] = {
    val client = new FTPClient()

    try {
      val server = con.uri.getHost
      val port: Int =
        if (con.uri.getPort > 0)
          con.uri.getPort
        else
          ftpPortNumber

      client.setDefaultPort(port)
      client.setConnectTimeout(ftpConnectionTimeout.toInt)
      client.connect(server)
      val reply = client.getReplyCode

      if (FTPReply.isPositiveCompletion(reply)) {
        val ftpCredentials = getFtpCredentials(con)
        val isLoggedIn =
          client.login(ftpCredentials.username, ftpCredentials.password)

        if (isLoggedIn) {
          val filename   = s"test_file_wt_${Random.alphanumeric.take(10).mkString}.txt"
          val remoteFile = client.listFiles(filename)
          if (remoteFile.isEmpty) {
            val fileStored = client.storeFile(
              filename,
              new ByteArrayInputStream(filename.getBytes(StandardCharsets.UTF_8))
            )
            if (fileStored) {
              val _ = client.deleteFile(filename)
              con.successNel
            } else {
              s"Error during FTP connection for writeable network file `${con.uri}` - could not store file".failNel
            }
          } else {
            s"Error during FTP connection for writeable network file `${con.uri}` - test file already exists".failNel
          }

        } else { // not logged in
          client.disconnect()
          s"Error during FTP connection for writeable network file `${con.uri}` - could not connect".failNel
        }
      } else { // no connection
        client.disconnect()
        s"Error during FTP connection for writeable network file `${con.uri}` with reply code: $reply".failNel
      }
    } finally {
      if (client.isConnected)
        client.disconnect()
    }
  }

  private def testWriteableFtpsFile(
      con: ConnectionInformation
  ): ValidationNel[String, ConnectionInformation] = {
    val client = new FTPSClient()

    try {
      val server = con.uri.getHost
      val port =
        if (con.uri.getPort > 0)
          con.uri.getPort
        else
          ftpsPortNumber

      client.setDefaultPort(port)
      client.setConnectTimeout(ftpConnectionTimeout.toInt)
      client.connect(server)
      val reply = client.getReplyCode

      if (FTPReply.isPositiveCompletion(reply)) {
        val ftpCredentials = getFtpCredentials(con)
        val isLoggedIn =
          client.login(ftpCredentials.username, ftpCredentials.password)

        if (isLoggedIn) {
          val filename   = s"test_file_wt_${Random.alphanumeric.take(10).mkString}.txt"
          val remoteFile = client.listFiles(filename)
          if (remoteFile.isEmpty) {
            val fileStored = client.storeFile(
              filename,
              new ByteArrayInputStream(filename.getBytes(StandardCharsets.UTF_8))
            )
            if (fileStored) {
              val _ = client.deleteFile(filename)
              con.successNel
            } else {
              s"Error during FTPs connection for writeable network file `${con.uri}` - could not store file".failNel
            }
          } else {
            s"Error during FTPs connection for writeable network file `${con.uri}` - test file already exists".failNel
          }

        } else { // not logged in
          client.disconnect()
          s"Error during FTPs connection for writeable network file `${con.uri}` - could not connect".failNel
        }
      } else { // no connection
        client.disconnect()
        s"Error during FTPs connection for writeable network file `${con.uri}` with reply code: $reply".failNel
      }
    } finally {
      if (client.isConnected)
        client.disconnect()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  @throws[JSchException]
  @throws[SftpException]
  @throws[IOException]
  private def testWriteableSftpFile(
      con: ConnectionInformation
  ): ValidationNel[String, ConnectionInformation] = {
    val host = con.uri.getHost
    val port =
      if (con.uri.getPort > 0)
        con.uri.getPort
      else
        sftpPortNumber
    val credentials = getFtpCredentials(con)

    val jsch             = new JSch()
    val session: Session = jsch.getSession(credentials.username, host, port)
    session.setPassword(credentials.password)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setTimeout(sftpConnectionTimeout.toInt)
    session.connect()
    if (session.isConnected) {
      val sftpChannel: ChannelSftp = session.openChannel("sftp").asInstanceOf[ChannelSftp]
      sftpChannel.connect()
      if (sftpChannel.isConnected) {
        val filename    = s"test_file_wt_${Random.alphanumeric.take(10).mkString}.txt"
        val inputStream = sftpChannel.get(filename)
        if (inputStream == null) {
          sftpChannel.put(filename, filename)
          val newInput = sftpChannel.get(filename)
          if (newInput != null) {
            sftpChannel.rm(filename)
            con.successNel
          } else {
            s"Error during SFTP connection for writeable network file `${con.uri}` : could not write".failNel
          }
        } else {
          if (session.isConnected)
            session.disconnect()
          s"Error during SFTP connection for writeable network file `${con.uri}` : test file already existed".failNel
        }
      } else {
        if (session.isConnected)
          session.disconnect()
        s"Error during SFTP connection for writeable network file `${con.uri}` : channel not connected".failNel
      }
    } else
      s"Error during SFTP connection for writeable network file `${con.uri}` : session not connected".failNel
  }

  private def checkAccessDatabase(
      con: ConnectionInformation,
      writeable: Boolean
  ): ValidationNel[String, ConnectionInformation] =
    connect(con) match {
      case -\/(failure) => GenericHelpers.createValidationFromException(failure)
      case \/-(success) =>
        if (writeable) {
          Try {
            val stm       = success.createStatement()
            val tablename = s"T${Random.alphanumeric.take(8).mkString}"
            val ignoreA   = stm.execute(s"CREATE TABLE $tablename (test CHAR(18))")
            val ignoreB   = stm.execute(s"INSERT INTO $tablename (test) VALUES('TENSEI-WRITE-TEST')")
            val ignoreC   = stm.execute(s"DROP TABLE $tablename")
            stm.close()
            success.close()
          } match {
            case scala.util.Failure(f) => GenericHelpers.createValidationFromException(f)
            case scala.util.Success(_) => con.successNel
          }
        } else {
          success.close()
          con.successNel
        }
    }

  private def checkAccessAPI(con: ConnectionInformation,
                             writeable: Boolean): ValidationNel[String, ConnectionInformation] =
    "APIs are not yet implemented!".failNel

  private def checkAccessStream(con: ConnectionInformation,
                                writeable: Boolean): ValidationNel[String, ConnectionInformation] =
    "Streams are not yet implemented!".failNel
}
