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

import akka.actor.ActorRef
import akka.testkit.{ EventFilter, TestActorRef }
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

class Drupal2WP extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "drupal"
  val targetDatabaseName = "wordpress"

  var agent: ActorRef = null

  override def beforeEach(): Unit = {
    val c =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    c.close()
    agent = createDummyAgent(Option("Drupal2Wp-Test"))
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
    EventFilter.debug(message = "stopped", source = agent.path.toString, occurrences = 1) intercept stopDummyAgent()
  }

  private def executeDbQuery(db: java.sql.Connection, sql: String): Unit = {
    val s = db.createStatement()
    s.execute(sql)
    s.close()
  }

  describe("Drupal to Wordpress") {
    describe("using databases") {
      describe("Drupal V 7.39 / Wordpress V 4.3") {
        describe("migrating users to wp_users") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE users (
                |  uid int(10),
                |  name varchar(60),
                |  pass varchar(128),
                |  mail varchar(254),
                |  theme varchar(255),
                |  signature varchar(255),
                |  signature_format varchar(255),
                |  created int(11),
                |  access int(11),
                |  login int(11),
                |  status int(4),
                |  timezone varchar(32),
                |  language varchar(12),
                |  picture int(11),
                |  init varchar(254),
                |  data varchar
                |);
                |INSERT INTO users VALUES(0, '', '', '', '', '', NULL, 0, 0, 0, 0, NULL, '', 0, '', NULL);
                |INSERT INTO users VALUES(1, 'admin', '$S$D2Lz7Va9sE.ukK/qzyhkX23DACBX6iFzip3wB1iQ7tnVB5dSafMQ', 'test@localhost.de', '', '', NULL, 1441196805, 1441197933, 1441196869, 1, 'Europe/Berlin', '', 0, 'test@localhost.de', 0x623a303b);
                |INSERT INTO users VALUES(2, 'andre', '$S$DAADq1F6msAXrr5jLuKFHeMVW8G/B8oQo73.Kn0jl/iYmRTFTAqu', 'andre@localhost.de', '', '', 'filtered_html', 1441197462, 1441197998, 1441197998, 1, 'Europe/Berlin', '', 0, 'andre@localhost.de', NULL);
                |INSERT INTO users VALUES(3, 'jens', '$S$DZOH5WEzRemDZcnm8xsKfIOPqlgqESriJOWVnKB0J0un.EPQLy2j', 'jens@localhost.de', '', '', 'filtered_html', 1441197476, 1441197952, 1441197952, 1, 'Europe/Berlin', '', 0, 'jens@localhost.de', NULL);
                |INSERT INTO users VALUES(4, 'frank', '$S$DSXwzTIj.NgsM3lQXM3ISuIiAAMz9H2fcJEGCSq46Jt7Qmjgy.XK', 'frank@localhost.de', '', '', 'filtered_html', 1441197489, 1441198082, 1441198082, 1, 'Europe/Berlin', '', 0, 'frank@localhost.de', NULL);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-source-drupal_users.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-target-wp_users.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Drupal-Users",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "users_row_uid")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_id"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "users_row_mail")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_user_email")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.Replace",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("search", "^$"),
                                                  ("replace", "TEST@EXAMPLE.COM")))
                        )
                      )
                    ),
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "users_row_name"),
                        ElementReference(sourceDfasdl.id, "users_row_name"),
                        ElementReference(sourceDfasdl.id, "users_row_name")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_users_row_user_login"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_nicename"),
                        ElementReference(targetDfasdl.id, "wp_users_row_display_name")
                      ),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "^$"),
                                                                          ("replace",
                                                                           "anonymous"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "users_row_uid"),
                           ElementReference(sourceDfasdl.id, "users_row_uid"),
                           ElementReference(sourceDfasdl.id, "users_row_uid")),
                      List(
                        ElementReference(targetDfasdl.id, "wp_users_row_user_pass"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_url"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_activation_key")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "users_row_uid")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_user_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "users_row_created")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_user_registered")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      ),
                      List(
                        AtomicTransformationDescription(
                          ElementReference(sourceDfasdl.id, "users_row_created"),
                          "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
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
              DataTreeDocument.props(sourceDfasdl, Option("Drupal2Wp"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Drupal2Wp")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "users"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(4)

            agent ! DummyAgent.CreateProcessor(Option("Drupal2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_users ORDER BY id;")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getString("user_login") should be("admin")
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.getString("user_login") should be("andre")
              results.getString("user_pass") should be("")
              results.getString("user_nicename") should be("andre")
              results.getString("user_email") should be("andre@localhost.de")
              results.getString("user_url") should be("")
              results.getTimestamp("user_registered") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:37:42.0")
              )
              results.getString("user_activation_key") should be("")
              results.getLong("user_status") should be(0)
              results.getString("display_name") should be("andre")
              results.next() should be(right = true)
              results.getLong("id") should be(3)
              results.getString("user_login") should be("jens")
              results.next() should be(right = true)
              results.getLong("id") should be(4)
              results.getString("user_login") should be("frank")
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating node to posts") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE node (
                |  nid int(10),
                |  vid int(10),
                |  type varchar(32),
                |  language varchar(12),
                |  title varchar(255),
                |  uid int(11),
                |  status int(11),
                |  created int(11),
                |  changed int(11),
                |  comment int(11),
                |  promote int(11),
                |  sticky int(11),
                |  tnid int(10),
                |  translate int(11)
                |);
                |INSERT INTO node VALUES(1, 1, 'article', 'und', 'Die ist ein einfacher Artikel', 2, 1, 1441197543, 1441283821, 2, 1, 0, 0, 0);
                |INSERT INTO node VALUES(2, 2, 'article', 'und', 'Migration mit Tensei-Data', 3, 1, 1441197796, 1441197796, 2, 1, 0, 0, 0);
                |INSERT INTO node VALUES(3, 3, 'article', 'und', 'Hundefutter ist wichtig', 4, 1, 1441197893, 1441197893, 2, 1, 0, 0, 0);
                |INSERT INTO node VALUES(4, 4, 'article', 'und', 'Dieser Artikel soll dann mal Kommentare enthalten', 2, 1, 1441197923, 1441197923, 2, 1, 0, 0, 0);
                |CREATE TABLE field_data_body (
                |  entity_type varchar(128),
                |  bundle varchar(128),
                |  deleted int(4),
                |  entity_id int(10),
                |  revision_id int(10),
                |  language varchar(32),
                |  delta int(10),
                |  body_value varchar,
                |  body_summary varchar,
                |  body_format varchar(255)
                |);
                |INSERT INTO field_data_body VALUES('node', 'article', 0, 1, 1, 'und', 0, 'Dieser Artikel enthält keinen besonderen Text und wurde mit einfachen Mitteln erstellt. Das Inhalt ist eher nebensächlich und kann getrost als uninteressant angesehen werden. Dennoch ist es wichtig, dass es diese Beispielinhalte gibt, denn nur so kann die Migration der Daten mit den Möglichkeiten von Tensei-Data gezeigt werden.\r\n\r\nAlso, schnell auf den Knopf drücken und der Migration zuschauen!', '', 'full_html');
                |INSERT INTO field_data_body VALUES('node', 'article', 0, 2, 2, 'und', 0, 'Tensei-Data ist ein Datenmigrationswerkzeug, welches die einfache Migration von Daten ermöglicht. Durch die intuitive Oberfläche wird eine einfache Migration der Daten per Drag&Drop durchgeführt.\r\n\r\nWenn Daten verändert werden müssen, können mittels Transformatoren verschiedene Transformationen auf den Daten ausgeführt werden, die je nach Anwendungsfall eine Vielzahl von unterschiedlichen Anpassungen ermöglichen.\r\n\r\nTesten Sie es aus!', '', 'plain_text');
                |INSERT INTO field_data_body VALUES('node', 'article', 0, 3, 3, 'und', 0, 'Damit es ihrem kleinen Vierbeiner gut geht, sollten sie immer darauf achten, ihm ein gesundes und vollwertiges Hundefutter zu kaufen. Nur so gewährleisten sie eine ausreichende Zufuhr von wichtigen Spurenelementen und Vitaminen.\r\n\r\nDenn nicht vergessen: Ein gesunder Hund braucht viel Sport und ein gesundes Essen.', '', 'filtered_html');
                |INSERT INTO field_data_body VALUES('node', 'article', 0, 4, 4, 'und', 0, 'Unter diesem Artikel soll es eine Vielzahl von Kommentaren geben. Dadurch kann sehr schön gezeigt werden, wie die Migration der Kommentare vonstatten geht.\r\n\r\nKommentare können einfache Antworten auf einen Artikel sein, oder verschachtelt untereinander vorkommen. Auch diese Verschachtelungen sollen ordnungsgemäß übernommen werden.\r\n\r\nNa dann, viel Spass mit der Kommentarfunktion.', '', 'filtered_html');
                |CREATE TABLE node_comment_statistics (
                |  nid int(10),
                |  cid int(11),
                |  last_comment_timestamp int(11),
                |  last_comment_name varchar(60),
                |  last_comment_uid int(11),
                |  comment_count int(10)
                |);
                |INSERT INTO node_comment_statistics VALUES(1, 0, 1441197543, NULL, 2, 0);
                |INSERT INTO node_comment_statistics VALUES(2, 0, 1441197796, NULL, 3, 0);
                |INSERT INTO node_comment_statistics VALUES(3, 0, 1441197893, NULL, 4, 0);
                |INSERT INTO node_comment_statistics VALUES(4, 5, 1441198100, '', 4, 5);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-source-drupal_node.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-target-wp_posts.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Drupal-Users",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_nid"),
                           ElementReference(sourceDfasdl.id, "node_row_uid")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_id"),
                           ElementReference(targetDfasdl.id, "wp_posts_row_post_author"))
                    ),
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "node_row_created"),
                        ElementReference(sourceDfasdl.id, "node_row_created"),
                        ElementReference(sourceDfasdl.id, "node_row_created"),
                        ElementReference(sourceDfasdl.id, "node_row_created")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_date"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_date_gmt"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_modified"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_modified_gmt")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      ),
                      List(
                        AtomicTransformationDescription(
                          ElementReference(sourceDfasdl.id, "node_row_created"),
                          "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_body_value"),
                           ElementReference(sourceDfasdl.id, "node_row_title")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_content"),
                           ElementReference(targetDfasdl.id, "wp_posts_row_post_title")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "^$"),
                                                                          ("replace", ""))))
                      )
                    ),
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "node_row_nid"),
                        ElementReference(sourceDfasdl.id, "node_row_nid"),
                        ElementReference(sourceDfasdl.id, "node_row_nid"),
                        ElementReference(sourceDfasdl.id, "node_row_nid"),
                        ElementReference(sourceDfasdl.id, "node_row_nid"),
                        ElementReference(sourceDfasdl.id, "node_row_nid")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_excerpt"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_password"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_to_ping"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_pinged"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_content_filtered"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_mime_type")
                      ),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_status")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "1"),
                                                                          ("replace",
                                                                           "publish")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "0"),
                                                                          ("replace", "pending"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_comment")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_comment_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "0"),
                                                                          ("replace", "closed")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "'1','2'"),
                                                                          ("replace", "open"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_nid")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_ping_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "open"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_nid"),
                           ElementReference(sourceDfasdl.id, "node_row_nid")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_parent"),
                           ElementReference(targetDfasdl.id, "wp_posts_row_menu_order")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_title")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_name")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search",
                                                                           "[^a-zA-Z0-9]+"),
                                                                          ("replace", "-"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_nid")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_guid")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_type")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_type")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "article"),
                                                                          ("replace", "post"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "node_row_comment_count")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_comment_count"))
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
              DataTreeDocument.props(sourceDfasdl, Option("Drupal2Wp"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Drupal2Wp")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "node"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(4)

            agent ! DummyAgent.CreateProcessor(Option("Drupal2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_posts ORDER BY id;")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getLong("post_author") should be(2)
              results.getTimestamp("post_date") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:39:03.0")
              )
              results.getTimestamp("post_date_gmt") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:39:03.0")
              )
              results.getString("post_content") should be(
                "Dieser Artikel enthält keinen besonderen Text und wurde mit einfachen Mitteln erstellt. Das Inhalt ist eher nebensächlich und kann getrost als uninteressant angesehen werden. Dennoch ist es wichtig, dass es diese Beispielinhalte gibt, denn nur so kann die Migration der Daten mit den Möglichkeiten von Tensei-Data gezeigt werden.\\r\\n\\r\\nAlso, schnell auf den Knopf drücken und der Migration zuschauen!"
              )
              results.getString("post_title") should be("Die ist ein einfacher Artikel")
              results.getString("post_status") should be("publish")
              results.getString("comment_status") should be("open")
              results.getString("ping_status") should be("open")
              results.getString("post_name") should be("Die-ist-ein-einfacher-Artikel")
              results.getString("post_type") should be("post")
              results.getLong("comment_count") should be(0)
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.getLong("post_author") should be(3)
              results.next() should be(right = true)
              results.getLong("id") should be(3)
              results.next() should be(right = true)
              results.getLong("id") should be(4)
              results.getLong("post_author") should be(2)
              results.getTimestamp("post_date") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:45:23.0")
              )
              results.getTimestamp("post_date_gmt") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:45:23.0")
              )
              results.getString("post_content") should be(
                "Unter diesem Artikel soll es eine Vielzahl von Kommentaren geben. Dadurch kann sehr schön gezeigt werden, wie die Migration der Kommentare vonstatten geht.\\r\\n\\r\\nKommentare können einfache Antworten auf einen Artikel sein, oder verschachtelt untereinander vorkommen. Auch diese Verschachtelungen sollen ordnungsgemäß übernommen werden.\\r\\n\\r\\nNa dann, viel Spass mit der Kommentarfunktion."
              )
              results.getString("post_title") should be(
                "Dieser Artikel soll dann mal Kommentare enthalten"
              )
              results.getString("post_status") should be("publish")
              results.getString("comment_status") should be("open")
              results.getString("ping_status") should be("open")
              results.getString("post_name") should be(
                "Dieser-Artikel-soll-dann-mal-Kommentare-enthalten"
              )
              results.getString("post_type") should be("post")
              results.getLong("comment_count") should be(5)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating comment to wp_comments") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE comment (
                |  cid int(11),
                |  pid int(11),
                |  nid int(11),
                |  uid int(11),
                |  subject varchar(64),
                |  hostname varchar(128),
                |  created int(11),
                |  changed int(11),
                |  status int(3),
                |  thread varchar(255),
                |  name varchar(60),
                |  mail varchar(64),
                |  homepage varchar(255),
                |  language varchar(12)
                |);
                |INSERT INTO comment VALUES(1, 0, 4, 3, 'das ist das erste Kommentar in der ersten Ebene', '::1', 1441197973, 1441197973, 1, '01/', 'jens', '', '', 'und');
                |INSERT INTO comment VALUES(2, 0, 4, 3, 'und dann der zweite Kommentar in der ersten Ebene', '::1', 1441197989, 1441197989, 1, '02/', 'jens', '', '', 'und');
                |INSERT INTO comment VALUES(3, 0, 4, 2, 'Ein Kommentar in der ersten', '::1', 1441198023, 1441198023, 1, '03/', 'andre', '', '', 'und');
                |INSERT INTO comment VALUES(4, 1, 4, 2, 'Antwort auf den ersten Kommentar ohne Comment-Text', '::1', 1441198074, 1441198074, 1, '01.00/', 'andre', '', '', 'und');
                |INSERT INTO comment VALUES(5, 4, 4, 4, 'zweite Antwort', '::1', 1441198100, 1441198100, 1, '01.00.00/', 'frank', '', '', 'und');
                |CREATE TABLE field_data_comment_body (
                |  entity_type varchar(128),
                |  bundle varchar(128),
                |  deleted int(4),
                |  entity_id int(10),
                |  revision_id int(10),
                |  language varchar(32),
                |  delta int(10),
                |  comment_body_value varchar,
                |  comment_body_format varchar(255)
                |);
                |INSERT INTO field_data_comment_body VALUES('comment', 'comment_node_article', 0, 1, 1, 'und', 0, 'Und hier steht der Inhalt des Kommentares.', 'filtered_html');
                |INSERT INTO field_data_comment_body VALUES('comment', 'comment_node_article', 0, 2, 2, 'und', 0, 'MIT INHALT', 'filtered_html');
                |INSERT INTO field_data_comment_body VALUES('comment', 'comment_node_article', 0, 3, 3, 'und', 0, 'Ein Kommentar in der ersten Ebene ohne Subject', 'filtered_html');
                |INSERT INTO field_data_comment_body VALUES('comment', 'comment_node_article', 0, 4, 4, 'und', 0, 'oder doch', 'filtered_html');
                |INSERT INTO field_data_comment_body VALUES('comment', 'comment_node_article', 0, 5, 5, 'und', 0, 'in der dritten Ebene vom ersten Kommentar', 'filtered_html');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-source-drupal_comment.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/drupal2wp/drupal2wp-d7_39-wp4_3-target-wp_comments.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Drupal-Comment",
                  MapAllToAll,
                  List(
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_cid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_id"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_nid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_post_id"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_name")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_author"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_mail")),
                      List(
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_email")
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_homepage")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_url"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_hostname")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_ip"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_status")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_approved"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_pid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_parent"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_uid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_user_id"))
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_created")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_date"),
                           ElementReference(targetDfasdl.id, "wp_comments_row_comment_date_gmt")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.DateConverter",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      ),
                      List(
                        AtomicTransformationDescription(
                          ElementReference(sourceDfasdl.id, "comment_row_created"),
                          "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_cid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_karma")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "0"),
                                                                          ("type", "long"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_subject"),
                           ElementReference(sourceDfasdl.id, "comment_row_comment_body_value")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_content")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Concat",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("separator", "\\n"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "comment_row_cid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_agent"),
                           ElementReference(targetDfasdl.id, "wp_comments_row_comment_type")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
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
              DataTreeDocument.props(sourceDfasdl, Option("Drupal2Wp"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Drupal2Wp")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            print(s"${message.toString}")
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "comment"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(5)

            agent ! DummyAgent.CreateProcessor(Option("Drupal2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_comments ORDER BY comment_id;")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("comment_id") should be(1)
              results.getLong("comment_post_id") should be(4)
              results.getString("comment_author") should be("jens")
              results.getString("comment_author_email") should be("")
              results.getString("comment_author_url") should be("")
              results.getString("comment_author_ip") should be("::1")
              results.getTimestamp("comment_date") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:46:13.0")
              )
              results.getTimestamp("comment_date_gmt") should be(
                java.sql.Timestamp.valueOf("2015-09-02 12:46:13.0")
              )
              results.getString("comment_content") should be(
                "das ist das erste Kommentar in der ersten Ebene\\nUnd hier steht der Inhalt des Kommentares."
              )
              results.getLong("comment_karma") should be(0)
              results.getString("comment_approved") should be("1")
              results.getString("comment_agent") should be("")
              results.getString("comment_type") should be("")
              results.getLong("comment_parent") should be(0)
              results.getLong("user_id") should be(3)
              results.next() should be(right = true)
              results.getLong("comment_id") should be(2)
              results.next() should be(right = true)
              results.getLong("comment_id") should be(3)
              results.next() should be(right = true)
              results.getLong("comment_id") should be(4)
              results.next() should be(right = true)
              results.getLong("comment_id") should be(5)
              results.getLong("comment_post_id") should be(4)
              results.getString("comment_author") should be("frank")
              results.getString("comment_content") should be(
                "zweite Antwort\\nin der dritten Ebene vom ersten Kommentar"
              )
              results.getLong("comment_karma") should be(0)
              results.getString("comment_approved") should be("1")
              results.getString("comment_agent") should be("")
              results.getString("comment_type") should be("")
              results.getLong("comment_parent") should be(4)
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
