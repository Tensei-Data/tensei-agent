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
import com.wegtam.tensei.adt.Recipe.MapOneToOne
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

class WP2Joomla extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "wordpress"
  val targetDatabaseName = "joomla"

  var agent: ActorRef = null

  override def beforeEach(): Unit = {
    val c =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    c.close()
    agent = createDummyAgent(Option("Wp2Joomla-Test"))
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

  describe("Wordpress to Joomla") {
    describe("using databases") {
      describe("Wordpress V 4.2.2 / Joomla V 3.4.3") {
        describe("migrating wp_users to joomla_users with aggregated mappings") {
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
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-source-wp_users.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-target-joomla_users.xml"
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
                      createElementReferenceList(
                        sourceDfasdl,
                        List("wp_users_row_user_login",
                             "wp_users_row_user_login",
                             "wp_users_row_user_email",
                             "wp_users_row_user_registered",
                             "wp_users_row_user_registered")
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List("joomla_users_row_name",
                             "joomla_users_row_username",
                             "joomla_users_row_email",
                             "joomla_users_row_registerdate",
                             "joomla_users_row_lastvisitdate")
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_id")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.IfThenElseNumeric",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("if", "x>0"),
                                                  ("then", "x=x+819"),
                                                  ("format", "num")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_password")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_activation")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_params")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_users_row_lastresettime")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("type", "datetime"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_otpkey")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_otep")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_users_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_block")),
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
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_sendemail")),
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
                      createElementReferenceList(targetDfasdl, List("joomla_users_row_resetcount")),
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
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_users_row_requirereset")),
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
              DataTreeDocument.props(sourceDfasdl, Option("WP2Joomla"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Joomla")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_users"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Joomla"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM joomla_users ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(820)
              results.getString("name") should be("chris")
              results.getString("username") should be("chris")
              results.getString("email") should be("christian@wegtam.com")
              results.getString("password") should be("")
              results.getLong("block") should be(0)
              results.getLong("sendEmail") should be(0)
              results.getString("registerDate") should be("2015-07-07 09:22:31.0")
              results.getString("lastvisitDate") should be("2015-07-07 09:22:31.0")
              results.getString("activation") should be("")
              results.getString("params") should be("")
              results.getString("lastResetTime") should be("1970-01-01 00:00:00.0")
              results.getLong("resetCount") should be(0)
              results.getString("otpKey") should be("")
              results.getString("otep") should be("")
              results.getLong("requireReset") should be(0)
              results.next() should be(right = true)
              results.getLong("id") should be(821)
              results.getString("name") should be("user2")
              results.next() should be(right = true)
              results.getLong("id") should be(822)
              results.getString("name") should be("user3")
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating wp_posts to joomla_content") {
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
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-source-wp_posts.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-target-joomla_content.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            // state: 0 - unpublished, 1 - published, 2 - archived, -2 - trashed
            // access: 1 - public, 2 - registered, 3 - special, 5 - guest, 6 - super user

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
                      createElementReferenceList(
                        sourceDfasdl,
                        List(
                          "wp_posts_row_id",
                          "wp_posts_row_post_title",
                          "wp_posts_row_post_content",
                          "wp_posts_row_post_date_gmt",
                          "wp_posts_row_post_modified_gmt",
                          "wp_posts_row_post_date_gmt"
                        )
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "joomla_content_row_id",
                          "joomla_content_row_title",
                          "joomla_content_row_introtext",
                          "joomla_content_row_created",
                          "joomla_content_row_modified",
                          "joomla_content_row_publish_up"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_post_author",
                                                      "wp_posts_row_post_author")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_content_row_created_by",
                                                      "joomla_content_row_modified_by")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.IfThenElseNumeric",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("if", "x>0"),
                                                  ("then", "x=x+819"),
                                                  ("format", "num")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_asset_id")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_title")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_alias")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "'\\.','!','\\?','\\s+'"),
                                                  ("replace", "-")))
                        ),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "-{2,}"),
                                                                          ("replace", "-")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "'^-', '-$'"),
                                                                          ("replace", ""))))
                      )
                    ), //Sonderzeichen durch '-' ersetzen, mehrfaches Auftreten von '-' eliminieren, '-' am Anfang und Ende entfernen
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_post_status")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_state")),
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
                                             List(("search", "'draft','inherit','auto-draft'"),
                                                  ("replace", "0")))
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_catid")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "2"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_version")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_ordering")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_access")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_hits")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_featured")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "1"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_content_row_language")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "*"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id")),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "joomla_content_row_fulltext",
                          "joomla_content_row_created_by_alias",
                          "joomla_content_row_metakey",
                          "joomla_content_row_metadesc",
                          "joomla_content_row_xreference"
                        )
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_posts_row_id", "wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_content_row_checked_out_time",
                                                      "joomla_content_row_publish_down")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("type", "datetime"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_content_row_checked_out")),
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
                                                 List("wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id",
                                                      "wp_posts_row_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_content_row_images",
                                                      "joomla_content_row_urls",
                                                      "joomla_content_row_attribs",
                                                      "joomla_content_row_metadata")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "{}"),
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
              DataTreeDocument.props(sourceDfasdl, Option("WP2Joomla"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Joomla")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_posts"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Joomla"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM joomla_content ORDER BY id")

            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getLong("asset_id") should be(0)
              results.getString("title") should be("Hallo Welt!")
              results.getString("alias") should be("Hallo-Welt")
              results.getString("introtext") should be(
                "Willkommen zur deutschen Version von WordPress. Dies ist der erste Beitrag. Du kannst ihn bearbeiten oder löschen. Um Spam zu vermeiden, geh doch gleich mal in den Pluginbereich und aktiviere die entsprechenden Plugins. So, und nun genug geschwafelt – jetzt nichts wie ran ans Bloggen!"
              )
              results.getString("fulltext") should be("")
              results.getLong("state") should be(1)
              results.getLong("catid") should be(2)                           // 2 - Uncategorised
              results.getString("created") should be("2015-07-07 09:22:31.0") //GMT-Zeit
              results.getLong("created_by") should be(820)
              results.getString("created_by_alias") should be("")
              results.getString("modified") should be("2015-07-24 10:13:00.0") //GMT-Zeit
              results.getLong("modified_by") should be(820)
              results.getLong("checked_out") should be(0)
              results.getString("checked_out_time") should be("1970-01-01 00:00:00.0")
              results.getString("publish_up") should be("2015-07-07 09:22:31.0")
              results.getString("publish_down") should be("1970-01-01 00:00:00.0")
              results.getString("images") should be("{}")
              results.getString("urls") should be("{}")
              results.getString("attribs") should be("{}")
              results.getLong("version") should be(1)
              results.getLong("ordering") should be(0)
              results.getString("metakey") should be("")
              results.getString("metadesc") should be("")
              results.getLong("access") should be(1)
              results.getLong("hits") should be(0)
              results.getString("metadata") should be("{}")
              results.getLong("featured") should be(1)
              results.getString("language") should be("*")
              results.getString("xreference") should be("")
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.next() should be(right = true)
              results.getLong("id") should be(4)
              results.next() should be(right = true)
              results.getLong("id") should be(7)
              results.next() should be(right = true)
              results.getLong("id") should be(9)
              results.getLong("state") should be(0)
            }
          }
        }

        describe("migrating wp_contents to joomla_jcomments") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE wp_comments (
                |  comment_ID bigint(20),
                |  comment_post_ID bigint(20),
                |  comment_author tinytext,
                |  comment_author_email varchar(100),
                |  comment_author_url varchar(200),
                |  comment_author_IP varchar(100),
                |  comment_date datetime,
                |  comment_date_gmt datetime,
                |  comment_content text,
                |  comment_karma int(11),
                |  comment_approved varchar(20),
                |  comment_agent varchar(255),
                |  comment_type varchar(20),
                |  comment_parent bigint(20),
                |  user_id bigint(20),
                |);
                |INSERT INTO wp_comments VALUES (1, 1, 'Mr WordPress', '', 'https://wordpress.org/', '', '2015-07-07 11:22:31', '2015-07-07 09:22:31', 'Hi, das ist ein Kommentar.\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. ', 0, '1', '', '', 0, 0);
                |INSERT INTO wp_comments VALUES (2, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:09', '2015-07-08 07:44:09', 'cooler artikel', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES (3, 9, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-08 09:44:34', '2015-07-08 07:44:34', 'hier kommentiert der admin noch selbst', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 1);
                |INSERT INTO wp_comments VALUES (4, 9, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:45:56', '2015-07-08 07:45:56', 'user comment', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES (5, 1, 'user2', 'a@b.de', '', '127.0.0.1', '2015-07-08 09:49:03', '2015-07-08 07:49:03', '[quote name="Christian"]1. Kommentar[/quote]<br />Antwortkommentar', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0', '', 0, 2);
                |INSERT INTO wp_comments VALUES (6, 1, 'chris', 'christian_tessnow@yahoo.de', '', '127.0.0.1', '2015-07-24 12:33:05', '2015-07-24 10:33:05', 'REPLY', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 1);
                |INSERT INTO wp_comments VALUES (7, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:36', '2015-07-27 07:32:36', 'Antwort2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 1, 0);
                |INSERT INTO wp_comments VALUES (8, 1, 'chris', 'c@t.de', '', '127.0.0.1', '2015-07-27 09:32:52', '2015-07-27 07:32:52', 'REPLY2', 0, '1', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0', '', 6, 0);
              """.stripMargin

            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-source-wp_comments.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/wp2joomla/wp2joomla-wp4_22-j3_43-target-joomla_jcomments.xml"
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
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List(
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_post_id",
                          "wp_comments_row_user_id",
                          "wp_comments_row_comment_author",
                          "wp_comments_row_comment_author",
                          "wp_comments_row_comment_author_email",
                          "wp_comments_row_comment_author_url",
                          "wp_comments_row_comment_content",
                          "wp_comments_row_comment_author_ip",
                          "wp_comments_row_comment_date_gmt",
                          "wp_comments_row_comment_approved"
                        )
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "joomla_jcomments_row_id",
                          "joomla_jcomments_row_object_id",
                          "joomla_jcomments_row_userid",
                          "joomla_jcomments_row_name",
                          "joomla_jcomments_row_username",
                          "joomla_jcomments_row_email",
                          "joomla_jcomments_row_homepage",
                          "joomla_jcomments_row_comment",
                          "joomla_jcomments_row_ip",
                          "joomla_jcomments_row_date",
                          "joomla_jcomments_row_published"
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(
                        sourceDfasdl,
                        List(
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id",
                          "wp_comments_row_comment_id"
                        )
                      ),
                      createElementReferenceList(
                        targetDfasdl,
                        List(
                          "joomla_jcomments_row_parent",
                          "joomla_jcomments_row_thread_id",
                          "joomla_jcomments_row_level",
                          "joomla_jcomments_row_isgood",
                          "joomla_jcomments_row_ispoor",
                          "joomla_jcomments_row_deleted",
                          "joomla_jcomments_row_subscribe",
                          "joomla_jcomments_row_source_id",
                          "joomla_jcomments_row_checked_out"
                        )
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
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_jcomments_row_path")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_jcomments_row_object_group")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "com_content"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl,
                                                 List("wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_id",
                                                      "wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_jcomments_row_object_params",
                                                      "joomla_jcomments_row_title",
                                                      "joomla_jcomments_row_source",
                                                      "joomla_jcomments_row_editor")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl, List("joomla_jcomments_row_lang")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "de-DE"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      createElementReferenceList(sourceDfasdl, List("wp_comments_row_comment_id")),
                      createElementReferenceList(targetDfasdl,
                                                 List("joomla_jcomments_row_checked_out_time")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Overwrite",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("value", "1970-01-01 00:00:00"),
                                                  ("type", "datetime")))
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
              DataTreeDocument.props(sourceDfasdl, Option("WP2Joomla"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("WP2Joomla")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "wp_comments"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(8)

            agent ! DummyAgent.CreateProcessor(Option("Wp2Joomla"))
            val processor = expectMsgType[ActorRef]
            processor ! StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM joomla_jcomments ORDER BY id")

            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getLong("parent") should be(0)
              results.getLong("thread_id") should be(0)
              results.getString("path") should be("0")
              results.getLong("level") should be(0)
              results.getLong("object_id") should be(1)
              results.getString("object_group") should be("com_content")
              results.getString("object_params") should be("")
              results.getString("lang") should be("de-DE")
              results.getLong("userid") should be(0)
              results.getString("name") should be("Mr WordPress")
              results.getString("username") should be("Mr WordPress")
              results.getString("email") should be("")
              results.getString("homepage") should be("https://wordpress.org/")
              results.getString("title") should be("")
              results.getString("comment") should be(
                "Hi, das ist ein Kommentar.\\nUm einen Kommentar zu löschen, melde dich einfach an und betrachte die Beitrags-Kommentare. Dort hast du die Möglichkeit sie zu löschen oder zu bearbeiten. "
              )
              results.getString("ip") should be("")
              results.getString("date") should be("2015-07-07 09:22:31.0")
              results.getLong("isgood") should be(0)
              results.getLong("ispoor") should be(0)
              results.getLong("published") should be(1)
              results.getLong("deleted") should be(0)
              results.getLong("subscribe") should be(0)
              results.getString("source") should be("")
              results.getLong("source_id") should be(0)
              results.getLong("checked_out") should be(0)
              results.getString("checked_out_time") should be("1970-01-01 00:00:00.0")
              results.getString("editor") should be("")
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.next() should be(right = true)
              results.getLong("id") should be(3)
              results.next() should be(right = true)
              results.getLong("id") should be(4)
              results.next() should be(right = true)
              results.getLong("id") should be(5)
              results.next() should be(right = true)
              results.getLong("id") should be(6)
              results.next() should be(right = true)
              results.getLong("id") should be(7)
              results.next() should be(right = true)
              results.getLong("id") should be(8)
            }
          }
        }
      }
    }
  }
}
