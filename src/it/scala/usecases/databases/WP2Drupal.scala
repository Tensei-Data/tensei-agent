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

package usecases.databases

import java.io.InputStream
import java.net.URI

import akka.actor.{ ActorRef, Terminated }
import akka.testkit.{ TestActorRef, TestProbe }
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt.Recipe.{ MapAllToAll, MapOneToOne }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.{
  GetSequenceRowCount,
  SequenceRowCount
}
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, DummyAgent, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.duration._

class WP2Drupal extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "wordpress"
  val targetDatabaseName = "drupal"

  var agent: ActorRef = null

  override def beforeEach(): Unit = {
    val c =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    c.close()
    agent = createDummyAgent(Option("Wp2Drupal-Test"))
  }

  override def afterEach(): Unit = {
    val t  = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
    val st = t.createStatement()
    st.execute("SHUTDOWN")
    st.close()
    t.close()
    val c  = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName")
    val cs = c.createStatement()
    cs.execute("SHUTDOWN")
    cs.close()
    c.close()
    val p = TestProbe()
    p.watch(agent)
    stopDummyAgent()
    val _ = p.expectMsgType[Terminated]
  }

  private def executeDbQuery(db: java.sql.Connection, sql: String): Unit = {
    val s = db.createStatement()
    s.execute(sql)
    s.close()
  }

  describe("Wordpress to Drupal") {
    describe("using databases") {
      describe("Wordpress V 4.22 / Drupal V 7.38") {
        describe("migrating wp_users to drupal_users with aggregated mappings") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_users (
                |  ID bigint,
                |  user_login varchar(60),
                |  user_pass varchar(64),
                |  user_nicename varchar(50),
                |  user_email varchar(100),
                |  user_url varchar(100),
                |  user_registered datetime,
                |  user_activation_key varchar(60),
                |  user_status int(11),
                |  display_name varchar(250)
                |);
                |INSERT INTO wp_users VALUES (1, 'chris', '$P$BLAHdQjsKTB4V/4IVmf2Z88SeyQ465.', 'chris', 'christian@wegtam.com', '', '2015-07-07 09:22:31', '', 0, 'chris');
                |INSERT INTO wp_users VALUES (2, 'user2', '$P$Bu3nB/Vz3ENH0iHgFmsUBjqzo9xel4/', 'user2', 'a@b.de', '', '2015-07-08 07:34:07', '', 0, 'user2');
                |INSERT INTO wp_users VALUES (3, 'user3', '$P$BTB/Z9hjf.aA5kf3llAknBA39nqg2u.', 'user3', 'c@d.de', '', '2015-07-08 07:34:48', '', 0, 'user3');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_users.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_users.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Users",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_user_email",
                                                      "wp_users_row_id",
                                                      "wp_users_row_user_login",
                                                      "wp_users_row_user_email")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_mail",
                                                      "drupal_users_row_uid",
                                                      "drupal_users_row_name",
                                                      "drupal_users_row_init"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_display_name",
                                                      "wp_users_row_display_name")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_data",
                                                      "drupal_users_row_signature_format")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_user_registered")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_created")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_language",
                                                      "drupal_users_row_pass",
                                                      "drupal_users_row_theme",
                                                      "drupal_users_row_signature")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_access",
                                                      "drupal_users_row_login",
                                                      "drupal_users_row_picture")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_timezone")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "\\d+"), ("replace", "Europe/Berlin")))
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_users"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM drupal_users ORDER BY uid")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("uid") should be(1)
              results.getString("name") should be("chris")
              results.getString("pass") should be("")
              results.getString("mail") should be("christian@wegtam.com")
              results.getString("theme") should be("")
              results.getString("signature") should be("")
              results.getString("signature_format") should be(null)
              results.getLong("created") should be(1436260951L)
              results.getLong("access") should be(0)
              results.getLong("login") should be(0)
              results.getLong("status") should be(1)
              results.getString("timezone") should be("Europe/Berlin")
              results.getString("language") should be("")
              results.getLong("picture") should be(0)
              results.getString("init") should be("christian@wegtam.com")
              results.getString("data") should be(null)
              results.next() should be(right = true)
              results.getLong("uid") should be(2)
              results.getString("name") should be("user2")
              results.next() should be(right = true)
              results.getLong("uid") should be(3)
              results.getString("name") should be("user3")
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_users to drupal_users and wp_comments to drupal_comment") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_users (
                |  ID bigint,
                |  user_login varchar(60),
                |  user_pass varchar(64),
                |  user_nicename varchar(50),
                |  user_email varchar(100),
                |  user_url varchar(100),
                |  user_registered datetime,
                |  user_activation_key varchar(60),
                |  user_status int(11),
                |  display_name varchar(250)
                |);
                |INSERT INTO wp_users VALUES (1, 'chris', '$P$BLAHdQjsKTB4V/4IVmf2Z88SeyQ465.', 'chris', 'christian@wegtam.com', '', '2015-07-07 09:22:31', '', 0, 'chris');
                |INSERT INTO wp_users VALUES (2, 'user2', '$P$Bu3nB/Vz3ENH0iHgFmsUBjqzo9xel4/', 'user2', 'a@b.de', '', '2015-07-08 07:34:07', '', 0, 'user2');
                |INSERT INTO wp_users VALUES (3, 'user3', '$P$BTB/Z9hjf.aA5kf3llAknBA39nqg2u.', 'user3', 'c@d.de', '', '2015-07-08 07:34:48', '', 0, 'user3');
                |CREATE TABLE wp_comments (
                |  comment_ID bigint,
                |  comment_post_ID bigint,
                |  comment_author varchar,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content varchar,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint,
                |  user_id bigint
                |);
                |INSERT INTO wp_comments VALUES(1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES(2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(4, 9, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', 'bitte löschen!', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES(7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES(8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:52', '2015-07-27 07:32:52', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_users-and-wp_comments.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_users-and-drupal_comment.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Users",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_user_email",
                                                      "wp_users_row_id",
                                                      "wp_users_row_user_login",
                                                      "wp_users_row_user_email")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_mail",
                                                      "drupal_users_row_uid",
                                                      "drupal_users_row_name",
                                                      "drupal_users_row_init"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_display_name",
                                                      "wp_users_row_display_name")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_data",
                                                      "drupal_users_row_signature_format")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_user_registered")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_created")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_language",
                                                      "drupal_users_row_pass",
                                                      "drupal_users_row_theme",
                                                      "drupal_users_row_signature")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_users_row_id",
                                                      "wp_users_row_id",
                                                      "wp_users_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_users_row_access",
                                                      "drupal_users_row_login",
                                                      "drupal_users_row_picture")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_users_row_timezone")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "\\d+"), ("replace", "Europe/Berlin")))
                        )
                      )
                    )
                  )
                ),
                Recipe(
                  "Comments",
                  MapAllToAll,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_cid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_parent")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_pid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_post_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_nid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_user_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_uid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_ip")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_hostname")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "^$"),
                                                                          ("replace", ""))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_approved")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_status"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_name"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_email")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_mail"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_url")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_homepage"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_date_gmt")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_created")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_date_gmt")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_changed")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_subject")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_language")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_post_id",
                                                      "wp_comments_row_comment_parent")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_thread")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_users"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_comments"))
            val countComments = expectMsgType[SequenceRowCount]
            countComments.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM drupal_users ORDER BY uid")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("uid") should be(1)
              results.getString("name") should be("chris")
              results.getString("pass") should be("")
              results.getString("mail") should be("christian@wegtam.com")
              results.getString("theme") should be("")
              results.getString("signature") should be("")
              results.getString("signature_format") should be(null)
              results.getLong("created") should be(1436260951L)
              results.getLong("access") should be(0)
              results.getLong("login") should be(0)
              results.getLong("status") should be(1)
              results.getString("timezone") should be("Europe/Berlin")
              results.getString("language") should be("")
              results.getLong("picture") should be(0)
              results.getString("init") should be("christian@wegtam.com")
              results.getString("data") should be(null)
              results.next() should be(right = true)
              results.getLong("uid") should be(2)
              results.getString("name") should be("user2")
              results.next() should be(right = true)
              results.getLong("uid") should be(3)
              results.getString("name") should be("user3")
            }

            val results2 = statement.executeQuery("SELECT * FROM drupal_comment ORDER BY cid")
            withClue("Data should have been written to the database!") {
              results2.next() should be(right = true)
              results2.getLong("cid") should be(1)
              results2.getLong("pid") should be(0)
              results2.getLong("nid") should be(1)
              results2.getLong("uid") should be(0)
              results2.getString("hostname") should be("")
              results2.getLong("status") should be(1)
              results2.getString("name") should be("Mr WordPress")
              results2.getString("mail") should be("")
              results2.getString("homepage") should be("https://wordpress.org/")
              results2.getLong("created") should be(1436260951L)
              results2.getLong("changed") should be(1436260951L)
              results2.getString("subject") should be("")
              results2.getString("language") should be("und")
              results2.getString("thread") should be("01/")
              results2.next() should be(right = true)
              results2.getLong("cid") should be(2)
              results2.getLong("pid") should be(0)
              results2.getLong("nid") should be(9)
              results2.getLong("uid") should be(1)
              results2.getString("hostname") should be("127.0.0.1")
              results2.getLong("status") should be(1)
              results2.getString("name") should be("chris")
              results2.getString("mail") should be("christian_tessnow@yahoo.de")
              results2.getString("homepage") should be("")
              results2.getLong("created") should be(1436341449L)
              results2.getLong("changed") should be(1436341449L)
              results2.getString("subject") should be("")
              results2.getString("language") should be("und")
              results2.getString("thread") should be("01/")
              results2.next() should be(right = true)
              results2.getLong("cid") should be(3)
              results2.getString("thread") should be("02/")
              results2.next() should be(right = true)
              results2.getLong("cid") should be(4)
              results2.getString("thread") should be("03/")
              results2.next() should be(right = true)
              results2.getLong("cid") should be(5)
              results2.next() should be(right = true)
              results2.getLong("cid") should be(6)
              results2.next() should be(right = true)
              results2.getLong("cid") should be(7)
              results2.next() should be(right = true)
              results2.getLong("cid") should be(8)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_posts to drupal_node") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_posts (
                |  ID bigint,
                |  post_author bigint,
                |  post_date datetime,
                |  post_date_gmt datetime,
                |  post_content varchar,
                |  post_title varchar,
                |  post_excerpt varchar,
                |  post_status varchar(20),
                |  comment_status varchar(20),
                |  ping_status varchar(20),
                |  post_password varchar(20),
                |  post_name varchar(200),
                |  to_ping varchar,
                |  pinged varchar,
                |  post_modified datetime,
                |  post_modified_gmt datetime,
                |  post_content_filtered varchar,
                |  post_parent bigint,
                |  guid varchar(255),
                |  menu_order int(11),
                |  post_type varchar(20),
                |  post_mime_type varchar(100),
                |  comment_count bigint
                |);
                |INSERT INTO wp_posts VALUES(1, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'publish', 'open', 'open', '', 'hallo-welt', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 0, 'http://localhost/wordpress/?p=1', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(2, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Dies ist ein Beispiel einer statischen Seite. Du kannst sie bearbeiten und beispielsweise Infos über dich oder das Weblog eingeben, damit die Leser wissen, woher du kommst und was du machst.\n\nDu kannst entweder beliebig viele Hauptseiten (wie diese hier) oder Unterseiten, die sich in der Hierachiestruktur den Hauptseiten unterordnen, anlegen. Du kannst sie auch alle innerhalb von WordPress ändern und verwalten.\n\nAls stolzer Besitzer eines neuen WordPress-Seite, solltest du zur Übersichtsseite, dem <a href="http://localhost/wordpress/wp-admin/">Dashboard</a> gehen, diese Seite löschen und damit loslegen, eigene Inhalte zu erstellen. Viel Spaß!', 'Beispiel-Seite', '', 'publish', 'open', 'open', '', 'beispiel-seite', '', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', '', 0, 'http://localhost/wordpress/?page_id=2', 0, 'page', '', 0);
                |INSERT INTO wp_posts VALUES(4, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'publish', 'open', 'open', '', 'testartikel', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 0, 'http://localhost/wordpress/?p=4', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(5, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:36:35', '2015-07-08 07:36:35', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(6, 1, '2015-07-08 09:37:31', '2015-07-08 07:37:31', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(7, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'publish', 'open', 'open', '', '2-testartikel', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 0, 'http://localhost/wordpress/?p=7', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(8, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'inherit', 'open', 'open', '', '7-revision-v1', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 7, 'http://localhost/wordpress/index.php/2015/07/08/7-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(9, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'draft', 'closed', 'closed', '', '3-testartikel', '', '', '2015-08-04 12:22:47', '2015-08-04 10:22:47', '', 0, 'http://localhost/wordpress/?p=9', 0, 'post', '', 3);
                |INSERT INTO wp_posts VALUES(10, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'inherit', 'open', 'open', '', '9-revision-v1', '', '', '2015-07-08 09:39:34', '2015-07-08 07:39:34', '', 9, 'http://localhost/wordpress/index.php/2015/07/08/9-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(12, 1, '2015-07-24 12:13:00', '2015-07-24 10:13:00', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'inherit', 'open', 'open', '', '1-revision-v1', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 1, 'http://localhost/wordpress/index.php/2015/07/24/1-revision-v1/', 0, 'revision', '', 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_posts.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_node.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            // FIXME
            // comments: 0 - no, 1 - read only, 2 - read/write

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Posts",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_post_author",
                                                      "wp_posts_row_post_title")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_row_nid",
                                                      "drupal_node_row_vid",
                                                      "drupal_node_row_uid",
                                                      "drupal_node_row_title"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_post_date_gmt",
                                                      "wp_posts_row_post_date_gmt")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_row_created",
                                                      "drupal_node_row_changed")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_status")),
                      createElementReferenceList(targetDfasdl, List("drupal_node_row_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "publish"),
                                                                          ("replace", "1")))),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "'inherit','auto-draft','draft'"),
                                                  ("replace", "0")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_comment_status")),
                      createElementReferenceList(targetDfasdl, List("drupal_node_row_comment")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "closed"),
                                                                          ("replace", "1")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "open"),
                                                                          ("replace", "2")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "spam"),
                                                                          ("replace", "0"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_node_row_type")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "post"),
                                                                          ("replace", "artikel")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "page"),
                                                                          ("replace", "seite"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_node_row_language")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_node_row_promote")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_post_mime_type",
                                                      "wp_posts_row_post_mime_type",
                                                      "wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_row_translate",
                                                      "drupal_node_row_tnid",
                                                      "drupal_node_row_sticky")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_posts"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            // FIXME
            // Merken: Je nach Sprache muss bei `type` im Ziel dann `artikel` oder `article` stehen und `seite` oder `page`

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM drupal_node ORDER BY nid")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("nid") should be(1)
              results.getLong("uid") should be(1)
              results.getLong("created") should be(1436260951L)
              results.getLong("changed") should be(1436260951L)
              results.getString("title") should be("Hallo Welt!")
              results.getLong("status") should be(1)
              results.getLong("comment") should be(2)
              results.getString("type") should be("artikel")
              results.getString("language") should be("und")
              results.getLong("promote") should be(1)
              results.getLong("sticky") should be(0)
              results.getLong("tnid") should be(0)
              results.getLong("translate") should be(0)
              results.getLong("vid") should be(1)
              results.next() should be(right = true)
              results.getLong("nid") should be(2)
              results.getString("type") should be("seite")
              results.next() should be(right = true)
              results.getLong("nid") should be(4)
              results.next() should be(right = true)
              results.getLong("nid") should be(7)
              results.next() should be(right = true)
              results.getLong("nid") should be(9)
              results.getLong("comment") should be(1)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_posts to drupal_node_revision") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_posts (
                |  ID bigint,
                |  post_author bigint,
                |  post_date datetime,
                |  post_date_gmt datetime,
                |  post_content varchar,
                |  post_title varchar,
                |  post_excerpt varchar,
                |  post_status varchar(20),
                |  comment_status varchar(20),
                |  ping_status varchar(20),
                |  post_password varchar(20),
                |  post_name varchar(200),
                |  to_ping varchar,
                |  pinged varchar,
                |  post_modified datetime,
                |  post_modified_gmt datetime,
                |  post_content_filtered varchar,
                |  post_parent bigint,
                |  guid varchar(255),
                |  menu_order int(11),
                |  post_type varchar(20),
                |  post_mime_type varchar(100),
                |  comment_count bigint
                |);
                |INSERT INTO wp_posts VALUES(1, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'publish', 'open', 'open', '', 'hallo-welt', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 0, 'http://localhost/wordpress/?p=1', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(2, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Dies ist ein Beispiel einer statischen Seite. Du kannst sie bearbeiten und beispielsweise Infos über dich oder das Weblog eingeben, damit die Leser wissen, woher du kommst und was du machst.\n\nDu kannst entweder beliebig viele Hauptseiten (wie diese hier) oder Unterseiten, die sich in der Hierachiestruktur den Hauptseiten unterordnen, anlegen. Du kannst sie auch alle innerhalb von WordPress ändern und verwalten.\n\nAls stolzer Besitzer eines neuen WordPress-Seite, solltest du zur Übersichtsseite, dem <a href="http://localhost/wordpress/wp-admin/">Dashboard</a> gehen, diese Seite löschen und damit loslegen, eigene Inhalte zu erstellen. Viel Spaß!', 'Beispiel-Seite', '', 'publish', 'open', 'open', '', 'beispiel-seite', '', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', '', 0, 'http://localhost/wordpress/?page_id=2', 0, 'page', '', 0);
                |INSERT INTO wp_posts VALUES(4, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'publish', 'open', 'open', '', 'testartikel', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 0, 'http://localhost/wordpress/?p=4', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(5, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:36:35', '2015-07-08 07:36:35', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(6, 1, '2015-07-08 09:37:31', '2015-07-08 07:37:31', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(7, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'publish', 'open', 'open', '', '2-testartikel', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 0, 'http://localhost/wordpress/?p=7', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(8, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'inherit', 'open', 'open', '', '7-revision-v1', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 7, 'http://localhost/wordpress/index.php/2015/07/08/7-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(9, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'draft', 'closed', 'closed', '', '3-testartikel', '', '', '2015-08-04 12:22:47', '2015-08-04 10:22:47', '', 0, 'http://localhost/wordpress/?p=9', 0, 'post', '', 3);
                |INSERT INTO wp_posts VALUES(10, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'inherit', 'open', 'open', '', '9-revision-v1', '', '', '2015-07-08 09:39:34', '2015-07-08 07:39:34', '', 9, 'http://localhost/wordpress/index.php/2015/07/08/9-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(12, 1, '2015-07-24 12:13:00', '2015-07-24 10:13:00', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'inherit', 'open', 'open', '', '1-revision-v1', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 1, 'http://localhost/wordpress/index.php/2015/07/24/1-revision-v1/', 0, 'revision', '', 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_posts.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_node_revision.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            // FIXME
            // comments: 0 - no, 1 - read only, 2 - read/write

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Posts-to-node-revision",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_post_author",
                                                      "wp_posts_row_post_title")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_nid",
                                                      "drupal_node_revision_row_vid",
                                                      "drupal_node_revision_row_uid",
                                                      "drupal_node_revision_row_title"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_log")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_date_gmt")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_timestamp")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_status")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "publish"),
                                                                          ("replace", "1")))),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "'inherit','auto-draft','draft'"),
                                                  ("replace", "0")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_comment_status")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_comment")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "closed"),
                                                                          ("replace", "1")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "open"),
                                                                          ("replace", "2")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "spam"),
                                                                          ("replace", "0"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_promote")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_node_revision_row_sticky")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_posts"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            // FIXME
            // Merken: Je nach Sprache muss bei `type` im Ziel dann `artikel` oder `article` stehen und `seite` oder `page`

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM drupal_node_revision ORDER BY nid")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("nid") should be(1)
              results.getLong("uid") should be(1)
              results.getLong("timestamp") should be(1436260951L)
              results.getString("title") should be("Hallo Welt!")
              results.getLong("status") should be(1)
              results.getLong("comment") should be(2)
              results.getLong("promote") should be(1)
              results.getLong("sticky") should be(0)
              results.getLong("vid") should be(1)
              results.getString("log") should be("")
              results.next() should be(right = true)
              results.getLong("nid") should be(2)
              results.next() should be(right = true)
              results.getLong("nid") should be(4)
              results.next() should be(right = true)
              results.getLong("nid") should be(7)
              results.next() should be(right = true)
              results.getLong("nid") should be(9)
              results.getLong("comment") should be(1)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_posts to drupal_field_data_body") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_posts (
                |  ID bigint,
                |  post_author bigint,
                |  post_date datetime,
                |  post_date_gmt datetime,
                |  post_content varchar,
                |  post_title varchar,
                |  post_excerpt varchar,
                |  post_status varchar(20),
                |  comment_status varchar(20),
                |  ping_status varchar(20),
                |  post_password varchar(20),
                |  post_name varchar(200),
                |  to_ping varchar,
                |  pinged varchar,
                |  post_modified datetime,
                |  post_modified_gmt datetime,
                |  post_content_filtered varchar,
                |  post_parent bigint,
                |  guid varchar(255),
                |  menu_order int(11),
                |  post_type varchar(20),
                |  post_mime_type varchar(100),
                |  comment_count bigint
                |);
                |INSERT INTO wp_posts VALUES(1, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'publish', 'open', 'open', '', 'hallo-welt', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 0, 'http://localhost/wordpress/?p=1', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(2, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Dies ist ein Beispiel einer statischen Seite. Du kannst sie bearbeiten und beispielsweise Infos über dich oder das Weblog eingeben, damit die Leser wissen, woher du kommst und was du machst.\n\nDu kannst entweder beliebig viele Hauptseiten (wie diese hier) oder Unterseiten, die sich in der Hierachiestruktur den Hauptseiten unterordnen, anlegen. Du kannst sie auch alle innerhalb von WordPress ändern und verwalten.\n\nAls stolzer Besitzer eines neuen WordPress-Seite, solltest du zur Übersichtsseite, dem <a href="http://localhost/wordpress/wp-admin/">Dashboard</a> gehen, diese Seite löschen und damit loslegen, eigene Inhalte zu erstellen. Viel Spaß!', 'Beispiel-Seite', '', 'publish', 'open', 'open', '', 'beispiel-seite', '', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', '', 0, 'http://localhost/wordpress/?page_id=2', 0, 'page', '', 0);
                |INSERT INTO wp_posts VALUES(4, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'publish', 'open', 'open', '', 'testartikel', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 0, 'http://localhost/wordpress/?p=4', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(5, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:36:35', '2015-07-08 07:36:35', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(6, 1, '2015-07-08 09:37:31', '2015-07-08 07:37:31', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(7, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'publish', 'open', 'open', '', '2-testartikel', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 0, 'http://localhost/wordpress/?p=7', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(8, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'inherit', 'open', 'open', '', '7-revision-v1', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 7, 'http://localhost/wordpress/index.php/2015/07/08/7-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(9, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'draft', 'closed', 'closed', '', '3-testartikel', '', '', '2015-08-04 12:22:47', '2015-08-04 10:22:47', '', 0, 'http://localhost/wordpress/?p=9', 0, 'post', '', 3);
                |INSERT INTO wp_posts VALUES(10, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'inherit', 'open', 'open', '', '9-revision-v1', '', '', '2015-07-08 09:39:34', '2015-07-08 07:39:34', '', 9, 'http://localhost/wordpress/index.php/2015/07/08/9-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(12, 1, '2015-07-24 12:13:00', '2015-07-24 10:13:00', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'inherit', 'open', 'open', '', '1-revision-v1', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 1, 'http://localhost/wordpress/index.php/2015/07/24/1-revision-v1/', 0, 'revision', '', 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_posts.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_field_data_body.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            // FIXME
            // in `drupal_field_data_body_row_body_format` setzen wir `php_code`, das kann man anpassen, abhängig, was man möchte (`html`, `full_html`)
            // Je nach Sprache muss bei `type` im Ziel dann `artikel` oder `article` stehen und `seite` oder `page`

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Posts",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_post_content",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_body_value",
                                                      "drupal_field_data_body_row_entity_id",
                                                      "drupal_field_data_body_row_revision_id"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_body_summary")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_entity_type")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "node"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_bundle")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "post"),
                                                                          ("replace", "artikel")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "page"),
                                                                          ("replace", "seite"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_post_mime_type",
                                                      "wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_delta",
                                                      "drupal_field_data_body_row_deleted")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_language")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_mime_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_body_row_body_format")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "full_html"),
                                                                          ("type", "string"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_posts"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results =
              statement.executeQuery("SELECT * FROM drupal_field_data_body ORDER BY entity_id")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("entity_id") should be(1)
              results.getString("body_value") should be(
                "Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!"
              )
              results.getLong("revision_id") should be(1)
              results.getString("body_summary") should be(null)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("artikel")
              results.getLong("deleted") should be(0)
              results.getString("language") should be("und")
              results.getLong("delta") should be(0)
              results.getString("body_format") should be("full_html")
              results.next() should be(right = true)
              results.getLong("entity_id") should be(2)
              results.getString("bundle") should be("seite")
              results.next() should be(right = true)
              results.getLong("entity_id") should be(4)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(7)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(9)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_comments to drupal_comment") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_comments (
                |  comment_ID bigint,
                |  comment_post_ID bigint,
                |  comment_author varchar,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content varchar,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint,
                |  user_id bigint
                |);
                |INSERT INTO wp_comments VALUES(1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES(2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(4, 9, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', 'bitte löschen!', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES(7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES(8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:52', '2015-07-27 07:32:52', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_comments.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_comment.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Comments",
                  MapAllToAll,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_cid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_parent")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_pid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_post_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_nid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_user_id")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_uid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_ip")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_hostname")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "^$"),
                                                                          ("replace", ""))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_approved")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_status"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_name"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_email")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_mail"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_author_url")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_homepage"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_date_gmt")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_created")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_date_gmt")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_changed")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_subject")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_language")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_post_id",
                                                      "wp_comments_row_comment_parent")),
                      createElementReferenceList(targetDfasdl, List("drupal_comment_row_thread")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_comments"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM drupal_comment ORDER BY cid")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("cid") should be(1)
              results.getLong("pid") should be(0)
              results.getLong("nid") should be(1)
              results.getLong("uid") should be(0)
              results.getString("hostname") should be("")
              results.getLong("status") should be(1)
              results.getString("name") should be("Mr WordPress")
              results.getString("mail") should be("")
              results.getString("homepage") should be("https://wordpress.org/")
              results.getLong("created") should be(1436260951L)
              results.getLong("changed") should be(1436260951L)
              results.getString("subject") should be("")
              results.getString("language") should be("und")
              results.getString("thread") should be("01/")
              results.next() should be(right = true)
              results.getLong("cid") should be(2)
              results.getLong("pid") should be(0)
              results.getLong("nid") should be(9)
              results.getLong("uid") should be(1)
              results.getString("hostname") should be("127.0.0.1")
              results.getLong("status") should be(1)
              results.getString("name") should be("chris")
              results.getString("mail") should be("christian_tessnow@yahoo.de")
              results.getString("homepage") should be("")
              results.getLong("created") should be(1436341449L)
              results.getLong("changed") should be(1436341449L)
              results.getString("subject") should be("")
              results.getString("language") should be("und")
              results.getString("thread") should be("01/")
              results.next() should be(right = true)
              results.getLong("cid") should be(3)
              results.getString("thread") should be("02/")
              results.next() should be(right = true)
              results.getLong("cid") should be(4)
              results.getString("thread") should be("03/")
              results.next() should be(right = true)
              results.getLong("cid") should be(5)
              results.next() should be(right = true)
              results.getLong("cid") should be(6)
              results.next() should be(right = true)
              results.getLong("cid") should be(7)
              results.next() should be(right = true)
              results.getLong("cid") should be(8)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_comments to drupal_field_data_comment") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_comments (
                |  comment_ID bigint,
                |  comment_post_ID bigint,
                |  comment_author varchar,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content varchar,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint,
                |  user_id bigint
                |);
                |INSERT INTO wp_comments VALUES(1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES(2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(4, 2, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', 'bitte löschen!', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES(7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES(8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:52', '2015-07-27 07:32:52', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
                |CREATE TABLE wp_posts (
                |  ID bigint,
                |  post_author bigint,
                |  post_date datetime,
                |  post_date_gmt datetime,
                |  post_content varchar,
                |  post_title varchar,
                |  post_excerpt varchar,
                |  post_status varchar(20),
                |  comment_status varchar(20),
                |  ping_status varchar(20),
                |  post_password varchar(20),
                |  post_name varchar(200),
                |  to_ping varchar,
                |  pinged varchar,
                |  post_modified datetime,
                |  post_modified_gmt datetime,
                |  post_content_filtered varchar,
                |  post_parent bigint,
                |  guid varchar(255),
                |  menu_order int(11),
                |  post_type varchar(20),
                |  post_mime_type varchar(100),
                |  comment_count bigint
                |);
                |INSERT INTO wp_posts VALUES(1, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'publish', 'open', 'open', '', 'hallo-welt', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 0, 'http://localhost/wordpress/?p=1', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(2, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Dies ist ein Beispiel einer statischen Seite. Du kannst sie bearbeiten und beispielsweise Infos über dich oder das Weblog eingeben, damit die Leser wissen, woher du kommst und was du machst.\n\nDu kannst entweder beliebig viele Hauptseiten (wie diese hier) oder Unterseiten, die sich in der Hierachiestruktur den Hauptseiten unterordnen, anlegen. Du kannst sie auch alle innerhalb von WordPress ändern und verwalten.\n\nAls stolzer Besitzer eines neuen WordPress-Seite, solltest du zur Übersichtsseite, dem <a href="http://localhost/wordpress/wp-admin/">Dashboard</a> gehen, diese Seite löschen und damit loslegen, eigene Inhalte zu erstellen. Viel Spaß!', 'Beispiel-Seite', '', 'publish', 'open', 'open', '', 'beispiel-seite', '', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', '', 0, 'http://localhost/wordpress/?page_id=2', 0, 'page', '', 0);
                |INSERT INTO wp_posts VALUES(4, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'publish', 'open', 'open', '', 'testartikel', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 0, 'http://localhost/wordpress/?p=4', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(5, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:36:35', '2015-07-08 07:36:35', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(6, 1, '2015-07-08 09:37:31', '2015-07-08 07:37:31', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(7, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'publish', 'open', 'open', '', '2-testartikel', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 0, 'http://localhost/wordpress/?p=7', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(8, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'inherit', 'open', 'open', '', '7-revision-v1', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 7, 'http://localhost/wordpress/index.php/2015/07/08/7-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(9, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'draft', 'closed', 'closed', '', '3-testartikel', '', '', '2015-08-04 12:22:47', '2015-08-04 10:22:47', '', 0, 'http://localhost/wordpress/?p=9', 0, 'post', '', 3);
                |INSERT INTO wp_posts VALUES(10, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'inherit', 'open', 'open', '', '9-revision-v1', '', '', '2015-07-08 09:39:34', '2015-07-08 07:39:34', '', 9, 'http://localhost/wordpress/index.php/2015/07/08/9-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(12, 1, '2015-07-24 12:13:00', '2015-07-24 10:13:00', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'inherit', 'open', 'open', '', '1-revision-v1', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 1, 'http://localhost/wordpress/index.php/2015/07/24/1-revision-v1/', 0, 'revision', '', 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            // FIXME
            // die source xml kann auch bei dem vorherigen Rezept genutzt werden

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_comments-join-posts.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_field_data_comment_body.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            // FIXME
            // in `drupal_field_data_comment_body_row_comment_body_format` setzen wir `php_code`, das kann man anpassen, abhängig, was man möchte (`html`, `full_html`)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Comments-to-field-data-body",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_content")),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "drupal_field_data_comment_body_row_entity_id",
                          "drupal_field_data_comment_body_row_revision_id",
                          "drupal_field_data_comment_body_row_comment_body_value"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_comment_body_row_entity_type")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "comment"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_post_type")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_comment_body_row_bundle")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "post"),
                                                  ("replace", "comment_node_artikel")))
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "page"),
                                                  ("replace", "comment_node_seite")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type",
                                                      "wp_comments_row_comment_type")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_comment_body_row_deleted",
                                                      "drupal_field_data_comment_body_row_delta")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_comment_body_row_language")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_type")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_comment_body_row_comment_body_format")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "full_html"),
                                                                          ("type", "string"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_comments"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_field_data_comment_body ORDER BY entity_id"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("entity_id") should be(1)
              results.getString("entity_type") should be("comment")
              results.getString("bundle") should be("comment_node_artikel")
              results.getLong("deleted") should be(0)
              results.getLong("revision_id") should be(1)
              results.getString("language") should be("und")
              results.getLong("delta") should be(0)
              results.getString("comment_body_format") should be("full_html")
              results.next() should be(right = true)
              results.getLong("entity_id") should be(2)
              results.getString("comment_body_value") should be("cooler artikel")
              results.next() should be(right = true)
              results.getLong("entity_id") should be(3)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(4)
              results.getString("bundle") should be("comment_node_seite")
              results.getLong("revision_id") should be(4)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(5)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(6)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(7)
              results.next() should be(right = true)
              results.getLong("entity_id") should be(8)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe(
          "migrating wp_comments_count_nuller to get the NULL entries to drupal_node_comment_statistics"
        ) {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_posts (
                |  ID bigint,
                |  post_author bigint,
                |  post_date datetime,
                |  post_date_gmt datetime,
                |  post_content varchar,
                |  post_title varchar,
                |  post_excerpt varchar,
                |  post_status varchar(20),
                |  comment_status varchar(20),
                |  ping_status varchar(20),
                |  post_password varchar(20),
                |  post_name varchar(200),
                |  to_ping varchar,
                |  pinged varchar,
                |  post_modified datetime,
                |  post_modified_gmt datetime,
                |  post_content_filtered varchar,
                |  post_parent bigint,
                |  guid varchar(255),
                |  menu_order int(11),
                |  post_type varchar(20),
                |  post_mime_type varchar(100),
                |  comment_count bigint
                |);
                |INSERT INTO wp_posts VALUES(1, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'publish', 'open', 'open', '', 'hallo-welt', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 0, 'http://localhost/wordpress/?p=1', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(2, 1, '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Dies ist ein Beispiel einer statischen Seite. Du kannst sie bearbeiten und beispielsweise Infos über dich oder das Weblog eingeben, damit die Leser wissen, woher du kommst und was du machst.\n\nDu kannst entweder beliebig viele Hauptseiten (wie diese hier) oder Unterseiten, die sich in der Hierachiestruktur den Hauptseiten unterordnen, anlegen. Du kannst sie auch alle innerhalb von WordPress ändern und verwalten.\n\nAls stolzer Besitzer eines neuen WordPress-Seite, solltest du zur Übersichtsseite, dem <a href="http://localhost/wordpress/wp-admin/">Dashboard</a> gehen, diese Seite löschen und damit loslegen, eigene Inhalte zu erstellen. Viel Spaß!', 'Beispiel-Seite', '', 'publish', 'open', 'open', '', 'beispiel-seite', '', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', '', 0, 'http://localhost/wordpress/?page_id=2', 0, 'page', '', 0);
                |INSERT INTO wp_posts VALUES(4, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'publish', 'open', 'open', '', 'testartikel', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 0, 'http://localhost/wordpress/?p=4', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(5, 1, '2015-07-08 09:36:35', '2015-07-08 07:36:35', 'ednfjkfkdskndsklfe', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:36:35', '2015-07-08 07:36:35', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(6, 1, '2015-07-08 09:37:31', '2015-07-08 07:37:31', 'ednfjkfkdskndsklfe\r\n\r\nfknfsdnflkdsfklds', 'Testartikel', '', 'inherit', 'open', 'open', '', '4-revision-v1', '', '', '2015-07-08 09:37:31', '2015-07-08 07:37:31', '', 4, 'http://localhost/wordpress/index.php/2015/07/08/4-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(7, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'publish', 'open', 'open', '', '2-testartikel', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 0, 'http://localhost/wordpress/?p=7', 0, 'post', '', 0);
                |INSERT INTO wp_posts VALUES(8, 1, '2015-07-08 09:39:06', '2015-07-08 07:39:06', 'bla blubb', '2. Testartikel', '', 'inherit', 'open', 'open', '', '7-revision-v1', '', '', '2015-07-08 09:39:06', '2015-07-08 07:39:06', '', 7, 'http://localhost/wordpress/index.php/2015/07/08/7-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(9, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'draft', 'closed', 'closed', '', '3-testartikel', '', '', '2015-08-04 12:22:47', '2015-08-04 10:22:47', '', 0, 'http://localhost/wordpress/?p=9', 0, 'post', '', 3);
                |INSERT INTO wp_posts VALUES(10, 1, '2015-07-08 09:39:34', '2015-07-08 07:39:34', 'blubb bla', '3. Testartikel', '', 'inherit', 'open', 'open', '', '9-revision-v1', '', '', '2015-07-08 09:39:34', '2015-07-08 07:39:34', '', 9, 'http://localhost/wordpress/index.php/2015/07/08/9-revision-v1/', 0, 'revision', '', 0);
                |INSERT INTO wp_posts VALUES(12, 1, '2015-07-24 12:13:00', '2015-07-24 10:13:00', 'Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!', 'Hallo Welt!', '', 'inherit', 'open', 'open', '', '1-revision-v1', '', '', '2015-07-24 12:13:00', '2015-07-24 10:13:00', '', 1, 'http://localhost/wordpress/index.php/2015/07/24/1-revision-v1/', 0, 'revision', '', 0);
                |CREATE TABLE wp_comments (
                |  comment_ID bigint,
                |  comment_post_ID bigint,
                |  comment_author varchar,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content varchar,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint,
                |  user_id bigint
                |);
                |INSERT INTO wp_comments VALUES(1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES(2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(4, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', 'bitte löschen!', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES(7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES(8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 09:32:36', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_comments_count_nuller.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_node_comment_statistics.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Comment-count",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List(
                          "wp_comments_count_nuller_row_nid",
                          "wp_comments_count_nuller_row_cid",
                          "wp_comments_count_nuller_row_last_comment_name",
                          "wp_comments_count_nuller_row_last_comment_uid",
                          "wp_comments_count_nuller_row_comment_count"
                        )
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "drupal_node_comment_statistics_row_nid",
                          "drupal_node_comment_statistics_row_cid",
                          "drupal_node_comment_statistics_row_last_comment_name",
                          "drupal_node_comment_statistics_row_last_comment_uid",
                          "drupal_node_comment_statistics_row_comment_count"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List("wp_comments_count_nuller_row_last_comment_timestamp")
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_node_comment_statistics_row_last_comment_timestamp")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(
              ElementReference(sourceDfasdl.id, "wp_comments_count_nuller")
            )
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_node_comment_statistics ORDER BY nid ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("nid") should be(2)
              results.getLong("comment_count") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(4)
              results.getLong("comment_count") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(7)
              results.getLong("comment_count") should be(0)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe(
          "migrating wp_comments_count to get the not NULL entries to drupal_node_comment_statistics"
        ) {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_comments (
                |  comment_ID bigint,
                |  comment_post_ID bigint,
                |  comment_author varchar,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content varchar,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint,
                |  user_id bigint
                |);
                |INSERT INTO wp_comments VALUES(1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES(2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES(4, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', 'bitte löschen!', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES(6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES(7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES(8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 09:32:36', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_comments_count.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_node_comment_statistics.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Comment-count",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List(
                          "wp_comments_count_row_nid",
                          "wp_comments_count_row_cid",
                          "wp_comments_count_row_last_comment_name",
                          "wp_comments_count_row_last_comment_uid",
                          "wp_comments_count_row_comment_count"
                        )
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "drupal_node_comment_statistics_row_nid",
                          "drupal_node_comment_statistics_row_cid",
                          "drupal_node_comment_statistics_row_last_comment_name",
                          "drupal_node_comment_statistics_row_last_comment_uid",
                          "drupal_node_comment_statistics_row_comment_count"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List("wp_comments_count_row_last_comment_timestamp")
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_node_comment_statistics_row_last_comment_timestamp")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "reduce")))
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_comments_count"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(2)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_node_comment_statistics ORDER BY nid ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("nid") should be(1)
              results.getLong("cid") should be(8)
              results.getLong("last_comment_timestamp") should be(1437989556L)
              results.getString("last_comment_name") should be("chris")
              results.getLong("last_comment_uid") should be(0)
              results.getLong("comment_count") should be(6)
              results.next() should be(right = true)
              results.getLong("nid") should be(9)
              results.getLong("cid") should be(3)
              results.getLong("last_comment_timestamp") should be(1436348674L)
              results.getString("last_comment_name") should be("chris")
              results.getLong("last_comment_uid") should be(1)
              results.getLong("comment_count") should be(2)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_term_taxonomy + wp_terms nach taxonomy_vocabulary") {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_terms (
                |  term_id bigint(20),
                |  name varchar(200),
                |  slug varchar(200),
                |  term_group bigint(10)
                |);
                |INSERT INTO wp_terms VALUES(1, 'Uncategorized', 'uncategorized', 0);
                |INSERT INTO wp_terms VALUES(2, 'Category1', 'category1', 0);
                |INSERT INTO wp_terms VALUES(3, 'tag1', 'tag1', 0);
                |INSERT INTO wp_terms VALUES(4, 'tag2', 'tag2', 0);
                |INSERT INTO wp_terms VALUES(5, 'Category2', 'category2', 0);
                |CREATE TABLE wp_term_taxonomy (
                |  term_taxonomy_id bigint(20),
                |  term_id bigint(20),
                |  taxonomy varchar(32),
                |  description varchar,
                |  parent bigint(20),
                |  count bigint(20)
                |);
                |INSERT INTO wp_term_taxonomy VALUES(1, 1, 'category', '', 0, 4);
                |INSERT INTO wp_term_taxonomy VALUES(2, 2, 'category', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(3, 3, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(4, 4, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(5, 5, 'category', '', 0, 1);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_terms-and-wp_term_taxonomy.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_taxonomy_vocabulary.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Vocabularies",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_terms_row_term_id",
                                                      "wp_terms_row_description")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_taxonomy_vocabulary_row_vid",
                             "drupal_taxonomy_vocabulary_row_description")
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_terms_row_term_id",
                                                      "wp_terms_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_vocabulary_row_hierarchy",
                                                      "drupal_taxonomy_vocabulary_row_weight")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_vocabulary_row_module")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "taxonomy"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_row_name")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_vocabulary_row_name")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "Uncategorized"), ("replace", "Tags")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_row_name")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_taxonomy_vocabulary_row_machine_name")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "Uncategorized"), ("replace", "tags")))
                        ),
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.LowerOrUpper",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("perform", "lower")))
                        )
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_terms"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results =
              statement.executeQuery("SELECT * FROM drupal_taxonomy_vocabulary ORDER BY vid ASC")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("vid") should be(1)
              results.getString("name") should be("Tags")
              results.getString("machine_name") should be("tags")
              results.getString("description") should be("")
              results.getLong("hierarchy") should be(0)
              results.getString("module") should be("taxonomy")
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("vid") should be(2)
              results.getString("name") should be("Category1")
              results.getString("machine_name") should be("category1")
              results.getString("description") should be("")
              results.getLong("hierarchy") should be(0)
              results.getString("module") should be("taxonomy")
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("vid") should be(5)
              results.getString("name") should be("Category2")
              results.getString("machine_name") should be("category2")
              results.getString("description") should be("")
              results.getLong("hierarchy") should be(0)
              results.getString("module") should be("taxonomy")
              results.getLong("weight") should be(0)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_term_taxonomy + wp_terms nach taxonomy_term_data") {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_terms (
                |  term_id bigint(20),
                |  name varchar(200),
                |  slug varchar(200),
                |  term_group bigint(10)
                |);
                |INSERT INTO wp_terms VALUES(1, 'Uncategorized', 'uncategorized', 0);
                |INSERT INTO wp_terms VALUES(2, 'category1', 'category1', 0);
                |INSERT INTO wp_terms VALUES(3, 'tag1', 'tag1', 0);
                |INSERT INTO wp_terms VALUES(4, 'tag2', 'tag2', 0);
                |INSERT INTO wp_terms VALUES(5, 'category2', 'category2', 0);
                |CREATE TABLE wp_term_taxonomy (
                |  term_taxonomy_id bigint(20),
                |  term_id bigint(20),
                |  taxonomy varchar(32),
                |  description varchar,
                |  parent bigint(20),
                |  count bigint(20)
                |);
                |INSERT INTO wp_term_taxonomy VALUES(1, 1, 'category', '', 0, 4);
                |INSERT INTO wp_term_taxonomy VALUES(2, 2, 'category', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(3, 3, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(4, 4, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(5, 5, 'category', '', 0, 1);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_terms-and-wp_term_taxonomy-terme.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_taxonomy_term_data.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Tags",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_terms_tags_row_term_id",
                                                      "wp_terms_tags_row_name",
                                                      "wp_terms_tags_row_description")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_term_data_row_tid",
                                                      "drupal_taxonomy_term_data_row_name",
                                                      "drupal_taxonomy_term_data_row_description"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_tags_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_term_data_row_vid")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_tags_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_term_data_row_format")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_tags_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_term_data_row_weight")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_terms_tags"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results =
              statement.executeQuery("SELECT * FROM drupal_taxonomy_term_data ORDER BY tid ASC")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("tid") should be(1)
              results.getLong("vid") should be(1)
              results.getString("name") should be("Uncategorized")
              results.getString("description") should be("")
              results.getString("format") should be(null)
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(2)
              results.getLong("vid") should be(1)
              results.getString("name") should be("category1")
              results.getString("description") should be("")
              results.getString("format") should be(null)
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(3)
              results.getLong("vid") should be(1)
              results.getString("name") should be("tag1")
              results.getString("description") should be("")
              results.getString("format") should be(null)
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(4)
              results.getLong("vid") should be(1)
              results.getString("name") should be("tag2")
              results.getString("description") should be("")
              results.getString("format") should be(null)
              results.getLong("weight") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(5)
              results.getLong("vid") should be(1)
              results.getString("name") should be("category2")
              results.getString("description") should be("")
              results.getString("format") should be(null)
              results.getLong("weight") should be(0)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_term_relationships nach taxonomy_index") {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_term_relationships (
                |  object_id bigint(20),
                |  term_taxonomy_id bigint(20),
                |  term_order int(11)
                |);
                |INSERT INTO wp_term_relationships VALUES(8, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(11, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 2, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 3, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 4, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 5, 0);
                |INSERT INTO wp_term_relationships VALUES(15, 1, 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_term_relationships.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_taxonomy_index.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Taxonomie-to-nodes",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List("wp_term_relationships_row_object_id",
                             "wp_term_relationships_row_term_taxonomy_id")
                      ),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_index_row_nid",
                                                      "drupal_taxonomy_index_row_tid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_term_relationships_row_object_id",
                                                      "wp_term_relationships_row_object_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_index_row_sticky",
                                                      "drupal_taxonomy_index_row_created")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(
              ElementReference(sourceDfasdl.id, "wp_term_relationships")
            )
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_taxonomy_index ORDER BY nid ASC, tid ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("nid") should be(8)
              results.getLong("tid") should be(1)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(11)
              results.getLong("tid") should be(1)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(13)
              results.getLong("tid") should be(1)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(13)
              results.getLong("tid") should be(2)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(13)
              results.getLong("tid") should be(3)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(13)
              results.getLong("tid") should be(4)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(13)
              results.getLong("tid") should be(5)
              results.getLong("sticky") should be(0)
              results.next() should be(right = true)
              results.getLong("nid") should be(15)
              results.getLong("tid") should be(1)
              results.getLong("sticky") should be(0)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_term_hierarchie nach taxonomy_term_hierarchie") {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_terms (
                |  term_id bigint(20),
                |  name varchar(200),
                |  slug varchar(200),
                |  term_group bigint(10)
                |);
                |INSERT INTO wp_terms VALUES(1, 'Uncategorized', 'uncategorized', 0);
                |INSERT INTO wp_terms VALUES(2, 'category1', 'category1', 0);
                |INSERT INTO wp_terms VALUES(3, 'tag1', 'tag1', 0);
                |INSERT INTO wp_terms VALUES(4, 'tag2', 'tag2', 0);
                |INSERT INTO wp_terms VALUES(5, 'category2', 'category2', 0);
                |CREATE TABLE wp_term_taxonomy (
                |  term_taxonomy_id bigint(20),
                |  term_id bigint(20),
                |  taxonomy varchar(32),
                |  description varchar,
                |  parent bigint(20),
                |  count bigint(20)
                |);
                |INSERT INTO wp_term_taxonomy VALUES(1, 1, 'category', '', 0, 4);
                |INSERT INTO wp_term_taxonomy VALUES(2, 2, 'category', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(3, 3, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(4, 4, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(5, 5, 'category', '', 0, 1);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_terms-and-wp_term_taxonomy-terme.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_taxonomy_term_hierarchy.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Taxonomy-hierarchy",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_tags_row_term_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_taxonomy_term_hierarchy_row_tid"))
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_terms_tags_row_term_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_taxonomy_term_hierarchy_row_parent")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_terms_tags"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_taxonomy_term_hierarchy ORDER BY tid ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("tid") should be(1)
              results.getLong("parent") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(2)
              results.getLong("parent") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(3)
              results.getLong("parent") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(4)
              results.getLong("parent") should be(0)
              results.next() should be(right = true)
              results.getLong("tid") should be(5)
              results.getLong("parent") should be(0)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_term_relationships + wp_term_taxonomy nach field_data_field_tags") {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_term_relationships (
                |  object_id bigint(20),
                |  term_taxonomy_id bigint(20),
                |  term_order int(11)
                |);
                |INSERT INTO wp_term_relationships VALUES(8, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(11, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 2, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 3, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 4, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 5, 0);
                |INSERT INTO wp_term_relationships VALUES(15, 1, 0);
                |CREATE TABLE wp_term_taxonomy (
                |  term_taxonomy_id bigint(20),
                |  term_id bigint(20),
                |  taxonomy varchar(32),
                |  description varchar,
                |  parent bigint(20),
                |  count bigint(20)
                |);
                |INSERT INTO wp_term_taxonomy VALUES(1, 1, 'category', '', 0, 4);
                |INSERT INTO wp_term_taxonomy VALUES(2, 2, 'category', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(3, 3, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(4, 4, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(5, 5, 'category', '', 0, 1);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_term_relationships-and-wp_term_taxonomy.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_field_data_field_tags.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Taxonomy-hierarchy",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_term_fields_row_node_id",
                                                      "wp_term_fields_row_node_id",
                                                      "wp_term_fields_row_term_id",
                                                      "wp_term_fields_row_term_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "drupal_field_data_field_tags_row_entity_id",
                          "drupal_field_data_field_tags_row_revision_id",
                          "drupal_field_data_field_tags_row_field_tags_tid",
                          "drupal_field_data_field_tags_row_delta"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_field_tags_row_entity_type")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "node"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_data_field_tags_row_language")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_field_tags_row_deleted")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_contype")),
                      createElementReferenceList(targetDfasdl,
                                                 List("drupal_field_data_field_tags_row_bundle")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "post_tag"),
                                                                          ("replace", "article")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "page_tag"),
                                                                          ("replace", "page")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "category"),
                                                                          ("replace", "article"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_term_fields"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_field_data_field_tags ORDER BY field_tags_tid ASC, entity_id ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(8)
              results.getLong("revision_id") should be(8)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(11)
              results.getLong("revision_id") should be(11)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(15)
              results.getLong("revision_id") should be(15)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(2)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(2)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(3)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(3)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(4)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(4)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(5)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(5)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe(
          "migrating wp_term_relationships + wp_term_taxonomy nach field_revision_field_tags"
        ) {
          it("should migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_term_relationships (
                |  object_id bigint(20),
                |  term_taxonomy_id bigint(20),
                |  term_order int(11)
                |);
                |INSERT INTO wp_term_relationships VALUES(8, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(11, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 1, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 2, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 3, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 4, 0);
                |INSERT INTO wp_term_relationships VALUES(13, 5, 0);
                |INSERT INTO wp_term_relationships VALUES(15, 1, 0);
                |CREATE TABLE wp_term_taxonomy (
                |  term_taxonomy_id bigint(20),
                |  term_id bigint(20),
                |  taxonomy varchar(32),
                |  description varchar,
                |  parent bigint(20),
                |  count bigint(20)
                |);
                |INSERT INTO wp_term_taxonomy VALUES(1, 1, 'category', '', 0, 4);
                |INSERT INTO wp_term_taxonomy VALUES(2, 2, 'category', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(3, 3, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(4, 4, 'post_tag', '', 0, 1);
                |INSERT INTO wp_term_taxonomy VALUES(5, 5, 'category', '', 0, 1);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-source-wp_term_relationships-and-wp_term_taxonomy.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2drupal/wp2drupal-wp4_22-d7_38-target-drupal_field_revision_field_tags.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Taxonomy-hierarchy",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_term_fields_row_node_id",
                                                      "wp_term_fields_row_node_id",
                                                      "wp_term_fields_row_term_id",
                                                      "wp_term_fields_row_term_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "drupal_field_revision_field_tags_row_entity_id",
                          "drupal_field_revision_field_tags_row_revision_id",
                          "drupal_field_revision_field_tags_row_field_tags_tid",
                          "drupal_field_revision_field_tags_row_delta"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_revision_field_tags_row_entity_type")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "node"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_revision_field_tags_row_language")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "und"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_node_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_revision_field_tags_row_deleted")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_term_fields_row_contype")),
                      createElementReferenceList(
                        targetDfasdl,
                        List("drupal_field_revision_field_tags_row_bundle")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "post_tag"),
                                                                          ("replace", "article")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "page_tag"),
                                                                          ("replace", "page")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "category"),
                                                                          ("replace", "article"))))
                      )
                    )
                  )
                )
              )
            )

            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-SRCDFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DSTDFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("WP2Drupal"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Drupal")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_term_fields"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Drupal"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results = statement.executeQuery(
              "SELECT * FROM drupal_field_revision_field_tags ORDER BY field_tags_tid ASC, entity_id ASC"
            )
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(8)
              results.getLong("revision_id") should be(8)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(11)
              results.getLong("revision_id") should be(11)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(1)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(15)
              results.getLong("revision_id") should be(15)
              results.getString("language") should be("und")
              results.getLong("delta") should be(1)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(2)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(2)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(3)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(3)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(4)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(4)
              results.next() should be(right = true)
              results.getLong("field_tags_tid") should be(5)
              results.getString("entity_type") should be("node")
              results.getString("bundle") should be("article")
              results.getLong("deleted") should be(0)
              results.getLong("entity_id") should be(13)
              results.getLong("revision_id") should be(13)
              results.getString("language") should be("und")
              results.getLong("delta") should be(5)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }
      }
    }
  }
}
