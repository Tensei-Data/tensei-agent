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

class Joomla2WP extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "joomla"
  val targetDatabaseName = "wordpress"

  var agent: ActorRef = null

  override def beforeEach(): Unit = {
    val c =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    c.close()
    agent = createDummyAgent(Option("Joomla2Wp-Test"))
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

  describe("Joomla to Wordpress") {
    describe("using databases") {
      describe("Joomla V 3.4.3 / Wordpress V 4.2.2") {
        describe("migrating joomla_users to wp_users with aggregated mappings") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE joomla_users (
                |  id int(11),
                |  name varchar(255),
                |  username varchar(150),
                |  email varchar(100),
                |  password varchar(100),
                |  block tinyint(4),
                |  sendEmail tinyint(4),
                |  registerDate datetime,
                |  lastvisitDate datetime,
                |  activation varchar(100),
                |  params text,
                |  lastResetTime datetime,
                |  resetCount int(11),
                |  otpKey varchar(1000),
                |  otep varchar(1000),
                |  requireReset tinyint(4)
                |);
                |INSERT INTO joomla_users VALUES (820, 'Super User', 'chris', 'christian@wegtam.de', '$2y$10$T.vT9e.Dyy48PDJgSNZ58euuP2slrtZlPImoHQ1aFl6kXIhm/4X/a', 0, 1, '2015-07-07 10:01:57', '2015-09-02 11:15:14', '0', '', '1970-01-01 00:00:00', 0, '', '', 0);
                |INSERT INTO joomla_users VALUES (821, 'user2', 'user2', 'christian2@wegtam.com', '$2y$10$yEMWUofveEhmJnhFwHYnWuB8Z/C15sgS6i7MG3tTt9F6nvKUA4o/e', 0, 0, '2015-07-08 09:07:25', '2015-07-21 10:54:40', '', '{"admin_style":"","admin_language":"","language":"","editor":"","helpsite":"","timezone":""}', '1970-01-01 00:00:00', 0, '', '', 0);
                |INSERT INTO joomla_users VALUES (822, 'user3', 'user3', 'christian3@wegtam.com', '$2y$10$TZkTaB4XWNbzeAJKkecFHur656fepcs5KeE3z9K7jhu0CBmU.Mf3i', 0, 0, '2015-07-08 09:07:55', '2015-07-08 09:18:16', '', '{"admin_style":"","admin_language":"","language":"","editor":"","helpsite":"","timezone":""}', '1970-01-01 00:00:00', 0, '', '', 0);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-source-joomla_users.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-target-wp_users.xml"
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
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_users_row_username"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_username"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_email"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_registerdate"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_username")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_users_row_user_login"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_nicename"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_email"),
                        ElementReference(targetDfasdl.id, "wp_users_row_user_registered"),
                        ElementReference(targetDfasdl.id, "wp_users_row_display_name")
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_users_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_id")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.IfThenElseNumeric",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("if", "x>0"),
                                                  ("then", "x=x-819"),
                                                  ("format", "num")))
                        )
                      )
                    ),
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_users_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_users_row_id")
                      ),
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
                      List(ElementReference(sourceDfasdl.id, "joomla_users_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_users_row_user_status")),
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
              DataTreeDocument.props(sourceDfasdl, Option("Joomla2WP"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Joomla2WP")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "joomla_users"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(3)

            agent ! DummyAgent.CreateProcessor(Option("Joomla2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_users ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getString("user_login") should be("chris")
              results.getString("user_pass") should be("")
              results.getString("user_nicename") should be("chris")
              results.getString("user_email") should be("christian@wegtam.de")
              results.getString("user_url") should be("")
              results.getString("user_registered") should be("2015-07-07 10:01:57.0")
              results.getString("user_activation_key") should be("")
              results.getLong("user_status") should be(0)
              results.getString("display_name") should be("chris")
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.next() should be(right = true)
              results.getLong("id") should be(3)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating joomla_posts to wp_content") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE joomla_content (
                |  id int(10),
                |  asset_id int(10),
                |  title varchar(255),
                |  alias varchar(255),
                |  introtext mediumtext,
                |  fulltext mediumtext,
                |  state tinyint(3),
                |  catid int(10),
                |  created datetime,
                |  created_by int(10),
                |  created_by_alias varchar(255),
                |  modified datetime,
                |  modified_by int(10),
                |  checked_out int(10),
                |  checked_out_time datetime,
                |  publish_up datetime,
                |  publish_down datetime,
                |  images text,
                |  urls text,
                |  attribs varchar(5120),
                |  version int(10),
                |  ordering int(11),
                |  metakey text,
                |  metadesc text,
                |  access int(10),
                |  hits int(10),
                |  metadata text,
                |  featured tinyint(3),
                |  language char(7),
                |  xreference varchar(50)
                |);
                |INSERT INTO joomla_content VALUES (1, 54, 'Testartikel', 'testartikel', '<p>Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt.</p>', '', 1, 2, '2015-07-08 09:05:09', 820, '', '2015-09-02 11:49:44', 820, 0, '1970-01-01 00:00:00', '2015-07-08 09:05:09', '1970-01-01 00:00:00', '{"image_intro":"","float_intro":"","image_intro_alt":"","image_intro_caption":"","image_fulltext":"","float_fulltext":"","image_fulltext_alt":"","image_fulltext_caption":""}', '{"urla":false,"urlatext":"","targeta":"","urlb":false,"urlbtext":"","targetb":"","urlc":false,"urlctext":"","targetc":""}', '{"show_title":"","link_titles":"","show_tags":"","show_intro":"","info_block_position":"","show_category":"","link_category":"","show_parent_category":"","link_parent_category":"","show_author":"","link_author":"","show_create_date":"","show_modify_date":"","show_publish_date":"","show_item_navigation":"","show_icons":"","show_print_icon":"","show_email_icon":"","show_vote":"","show_hits":"","show_noauth":"","urls_position":"","alternative_readmore":"","article_layout":"","show_publishing_options":"","show_article_options":"","show_urls_images_backend":"","show_urls_images_frontend":""}', 11, 1, '', '', 4, 9, '{"robots":"","author":"","rights":"","xreference":""}', 1, '*', '');
                |INSERT INTO joomla_content VALUES (2, 55, '2. Artikel', '2-artikel', '<p>dwffsdsdf</p>', '', 1, 2, '2015-07-08 09:06:18', 820, '', '2015-08-03 12:52:31', 820, 0, '1970-01-01 00:00:00', '2015-07-08 09:06:18', '1970-01-01 00:00:00', '{"image_intro":"","float_intro":"","image_intro_alt":"","image_intro_caption":"","image_fulltext":"","float_fulltext":"","image_fulltext_alt":"","image_fulltext_caption":""}', '{"urla":false,"urlatext":"","targeta":"","urlb":false,"urlbtext":"","targetb":"","urlc":false,"urlctext":"","targetc":""}', '{"show_title":"","link_titles":"","show_tags":"","show_intro":"","info_block_position":"","show_category":"","link_category":"","show_parent_category":"","link_parent_category":"","show_author":"","link_author":"","show_create_date":"","show_modify_date":"","show_publish_date":"","show_item_navigation":"","show_icons":"","show_print_icon":"","show_email_icon":"","show_vote":"","show_hits":"","show_noauth":"","urls_position":"","alternative_readmore":"","article_layout":"","show_publishing_options":"","show_article_options":"","show_urls_images_backend":"","show_urls_images_frontend":""}', 1, -1, '', '', 1, 0, '{"robots":"","author":"","rights":"","xreference":""}', 1, '*', '');
                |INSERT INTO joomla_content VALUES (3, 0, '3. Artikel', '3-artikel', '<p>sdafsdfsdfsdfsd</p>', '', 1, 2, '2015-07-08 09:06:43', 820, '', '2015-07-08 09:17:34', 820, 0, '1970-01-01 00:00:00', '2015-07-08 09:06:43', '1970-01-01 00:00:00', '{"image_intro":"","float_intro":"","image_intro_alt":"","image_intro_caption":"","image_fulltext":"","float_fulltext":"","image_fulltext_alt":"","image_fulltext_caption":""}', '{"urla":false,"urlatext":"","targeta":"","urlb":false,"urlbtext":"","targetb":"","urlc":false,"urlctext":"","targetc":""}', '{"show_title":"","link_titles":"","show_tags":"","show_intro":"","info_block_position":"","show_category":"","link_category":"","show_parent_category":"","link_parent_category":"","show_author":"","link_author":"","show_create_date":"","show_modify_date":"","show_publish_date":"","show_item_navigation":"","show_icons":"","show_print_icon":"","show_email_icon":"","show_vote":"","show_hits":"","show_noauth":"","urls_position":"","alternative_readmore":"","article_layout":"","show_publishing_options":"","show_article_options":"","show_urls_images_backend":"","show_urls_images_frontend":""}', 2, 2, '', '', 1, 1, '{"robots":"","author":"","rights":"","xreference":""}', 1, '*', '');
                |INSERT INTO joomla_content VALUES (4, 58, 'Hallo Welt!', 'hallo-welt', '<p>bla blubb</p>', '', -2, 2, '2015-09-02 11:15:57', 820, '', '2015-09-02 11:24:23', 820, 0, '1970-01-01 00:00:00', '2015-09-02 11:15:57', '1970-01-01 00:00:00', '{"image_intro":"","float_intro":"","image_intro_alt":"","image_intro_caption":"","image_fulltext":"","float_fulltext":"","image_fulltext_alt":"","image_fulltext_caption":""}', '{"urla":false,"urlatext":"","targeta":"","urlb":false,"urlbtext":"","targetb":"","urlc":false,"urlctext":"","targetc":""}', '{"show_title":"","link_titles":"","show_tags":"","show_intro":"","info_block_position":"","show_category":"","link_category":"","show_parent_category":"","link_parent_category":"","show_author":"","link_author":"","show_create_date":"","show_modify_date":"","show_publish_date":"","show_item_navigation":"","show_icons":"","show_print_icon":"","show_email_icon":"","show_vote":"","show_hits":"","show_noauth":"","urls_position":"","alternative_readmore":"","article_layout":"","show_publishing_options":"","show_article_options":"","show_urls_images_backend":"","show_urls_images_frontend":""}', 6, 0, '', '', 1, 0, '{"robots":"","author":"","rights":"","xreference":""}', 0, '*', '');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-source-joomla_content.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-target-wp_posts.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Content",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_title"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_created"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_modified"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_introtext"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_created"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_modified")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_posts_row_id"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_title"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_date_gmt"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_modified_gmt"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_content"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_date"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_modified")
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_created_by")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_author")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.IfThenElseNumeric",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("if", "x>0"),
                                                  ("then", "x=x-819"),
                                                  ("format", "num")))
                        )
                      )
                    ),
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id")
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
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_state")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "1"),
                                                                          ("replace", "publish")))),
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Replace",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("search", "0"),
                                                                          ("replace", "draft"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_comment_status")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "closed"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_id")),
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
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_title")),
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
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_content_row_id")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_posts_row_post_parent"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_menu_order"),
                        ElementReference(targetDfasdl.id, "wp_posts_row_comment_count")
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
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_post_type")),
                      List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Overwrite",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     List(("value", "post"),
                                                                          ("type", "string"))))
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_content_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_posts_row_guid")),
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
              DataTreeDocument.props(sourceDfasdl, Option("Joomla2WP"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Joomla2WP")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "joomla_content"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(4)

            agent ! DummyAgent.CreateProcessor(Option("Joomla2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_posts ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("id") should be(1)
              results.getLong("post_author") should be(1)
              results.getString("post_date") should be("2015-07-08 09:05:09.0")
              results.getString("post_date_gmt") should be("2015-07-08 09:05:09.0")
              results.getString("post_content") should be(
                "<p>Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt.</p>"
              )
              results.getString("post_title") should be("Testartikel")
              results.getString("post_excerpt") should be("")
              results.getString("post_status") should be("publish")
              results.getString("comment_status") should be("closed")
              results.getString("ping_status") should be("open")
              results.getString("post_password") should be("")
              results.getString("post_name") should be("Testartikel")
              results.getString("to_ping") should be("")
              results.getString("pinged") should be("")
              results.getString("post_modified") should be("2015-09-02 11:49:44.0")
              results.getString("post_modified_gmt") should be("2015-09-02 11:49:44.0")
              results.getString("post_content_filtered") should be("")
              results.getLong("post_parent") should be(0)
              results.getString("guid") should be("")
              results.getLong("menu_order") should be(0)
              results.getString("post_type") should be("post")
              results.getString("post_mime_type") should be("")
              results.getLong("comment_count") should be(0)
              results.next() should be(right = true)
              results.getLong("id") should be(2)
              results.next() should be(right = true)
              results.getLong("id") should be(3)
              results.next() should be(right = true)
              results.getLong("id") should be(4)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("migrating joomla_jcomments to wp_comments") {
          it("should properly migrate the data correctly", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE joomla_jcomments (
                |  id int(11),
                |  parent int(11),
                |  thread_id int(11),
                |  path varchar(255),
                |  level tinyint(1),
                |  object_id int(11),
                |  object_group varchar(255),
                |  object_params text,
                |  lang varchar(255),
                |  userid int(11),
                |  name varchar(255),
                |  username varchar(255),
                |  email varchar(255),
                |  homepage varchar(255),
                |  title varchar(255),
                |  comment text,
                |  ip varchar(39),
                |  date datetime,
                |  isgood smallint(5),
                |  ispoor smallint(5),
                |  published tinyint(1),
                |  deleted tinyint(1),
                |  subscribe tinyint(1),
                |  source varchar(255),
                |  source_id int(11),
                |  checked_out int(11),
                |  checked_out_time datetime,
                |  editor varchar(50)
                |);
                |INSERT INTO joomla_jcomments VALUES (4, 0, 0, '0', 0, 2, 'com_content', '', 'en-GB', 0, 'Christian', 'Christian', 'christian@wegtam.com', '', '', '1. Kommentar 8)', '127.0.0.1', '2015-09-17 08:38:46', 0, 0, 1, 0, 0, '', 0, 0, '1970-01-01 00:00:00', '');
                |INSERT INTO joomla_jcomments VALUES (5, 0, 0, '0', 0, 2, 'com_content', '', 'en-GB', 0, 'anderer Christian', 'anderer Christian', 'christian7@wegtam.com', '', '', '["quote name=Christian"]1. Kommentar 8)[/quote]<br />Antwortkommentar', '127.0.0.1', '2015-09-17 08:39:39', 0, 0, 1, 0, 0, '', 0, 0, '1970-01-01 00:00:00', '');
                |INSERT INTO joomla_jcomments VALUES (6, 0, 0, '0', 0, 2, 'com_content', '', 'en-GB', 0, 'Christian', 'Christian', 'christian@wegtam.com', '', '', '#3', '127.0.0.1', '2015-09-17 08:40:21', 0, 0, 1, 0, 0, '', 0, 0, '1970-01-01 00:00:00', '');
                |INSERT INTO joomla_jcomments VALUES (7, 0, 0, '0', 0, 3, 'com_content', '', 'en-GB', 0, 'Christian', 'Christian', 'christian@wegtam.com', '', '', 'test', '127.0.0.1', '2015-09-17 09:10:53', 0, 0, 1, 0, 0, '', 0, 0, '1970-01-01 00:00:00', '');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val srcin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-source-joomla_comments.xml"
            )
            val srcxml       = scala.io.Source.fromInputStream(srcin).mkString
            val sourceDfasdl = DFASDL("MY-SRCDFASDL", srcxml)

            val dstin: InputStream = getClass.getResourceAsStream(
              "/usecases/databases/joomla2wp/joomla2wp-j3_43-wp4_22-target-wp_comments.xml"
            )
            val dstxml       = scala.io.Source.fromInputStream(dstin).mkString
            val targetDfasdl = DFASDL("MY-DSTDFASDL", dstxml)

            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(sourceDfasdl),
              Option(targetDfasdl),
              List(
                Recipe(
                  "Comment",
                  MapOneToOne,
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_id"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_object_id"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_name"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_email"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_homepage"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_ip"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_date"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_date"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_comment"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_isgood"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_published"),
                        ElementReference(sourceDfasdl.id, "joomla_jcomments_row_parent")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_id"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_post_id"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_author"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_email"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_url"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_author_ip"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_date"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_date_gmt"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_content"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_karma"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_approved"),
                        ElementReference(targetDfasdl.id, "wp_comments_row_comment_parent")
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_jcomments_row_id"),
                           ElementReference(sourceDfasdl.id, "joomla_jcomments_row_id")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_comment_agent"),
                           ElementReference(targetDfasdl.id, "wp_comments_row_comment_type")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.EmptyString",
                          TransformerOptions(classOf[String], classOf[String])
                        )
                      )
                    ),
                    MappingTransformation(
                      List(ElementReference(sourceDfasdl.id, "joomla_jcomments_row_userid")),
                      List(ElementReference(targetDfasdl.id, "wp_comments_row_user_id")),
                      List(
                        TransformationDescription(
                          "com.wegtam.tensei.agent.transformers.IfThenElseNumeric",
                          TransformerOptions(classOf[String],
                                             classOf[String],
                                             List(("if", "x>0"),
                                                  ("then", "x=x-819"),
                                                  ("else", "0"),
                                                  ("format", "num")))
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
              DataTreeDocument.props(sourceDfasdl, Option("Joomla2WP"), Set.empty[String])
            )
            val dbParser =
              TestActorRef(DatabaseParser.props(source, cookbook, dataTree, Option("Joomla2WP")))
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](10.seconds)
            message.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "joomla_jcomments"))
            val count = expectMsgType[SequenceRowCount]
            count.rows.getOrElse(0L) should be(4)

            agent ! DummyAgent.CreateProcessor(Option("Joomla2Wp"))
            val processor = expectMsgType[ActorRef]
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(30.seconds, Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM wp_comments ORDER BY comment_ID")
            withClue("Data should have been written to the database!") {
              results.next() should be(right = true)
              results.getLong("comment_ID") should be(4)
              results.getLong("comment_post_ID") should be(2)
              results.getString("comment_author") should be("Christian")
              results.getString("comment_author_email") should be("christian@wegtam.com")
              results.getString("comment_author_url") should be("")
              results.getString("comment_author_IP") should be("127.0.0.1")
              results.getString("comment_date") should be("2015-09-17 08:38:46.0")
              results.getString("comment_date_gmt") should be("2015-09-17 08:38:46.0")
              results.getString("comment_content") should be("1. Kommentar 8)")
              results.getLong("comment_karma") should be(0)
              results.getString("comment_approved") should be("1")
              results.getString("comment_agent") should be("")
              results.getString("comment_type") should be("")
              results.getLong("comment_parent") should be(0)
              results.getLong("user_id") should be(0)
              results.next() should be(right = true)
              results.getLong("comment_ID") should be(5)
              results.next() should be(right = true)
              results.getLong("comment_ID") should be(6)
              results.next() should be(right = true)
              results.getLong("comment_ID") should be(7)
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
