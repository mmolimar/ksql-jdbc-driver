package com.github.mmolimar.ksql.jdbc

import java.io.{ByteArrayInputStream, InputStream}
import java.sql.{ResultSet, SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.metastore.SerdeFactory
import io.confluent.ksql.metastore.model.{KeyField, KsqlStream, KsqlTopic}
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.{ExecutionPlan, KafkaTopicsList, QueryDescriptionEntity, QueryDescriptionList, _}
import io.confluent.ksql.rest.server.computation.CommandId
import io.confluent.ksql.schema.ksql.KsqlSchema
import io.confluent.ksql.serde.Format
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy
import javax.ws.rs.core.Response
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._


class KsqlStatementSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  "A KsqlStatement" when {

    val mockResponse = mock[Response]
    (mockResponse.getEntity _).expects.returns(mock[InputStream]).anyNumberOfTimes
    val mockedKsqlRestClient = mock[MockableKsqlRestClient]
    val statement = new KsqlStatement(mockedKsqlRestClient)

    "validating specs" should {

      "throw not supported exception if not supported" in {

        (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .noMoreThanOnce

        val methods = implementedMethods[KsqlStatement]
        reflectMethods[KsqlStatement](methods = methods, implemented = false, obj = statement)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work when executing queries" in {

        assertThrows[SQLException] {
          statement.getResultSet
        }
        assertThrows[SQLException] {
          statement.execute(null, -1)
        }
        assertThrows[SQLException] {
          statement.execute("", Array[Int]())
        }
        assertThrows[SQLException] {
          statement.execute("invalid query", Array[String]())
        }

        assertThrows[SQLException] {
          (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.erroneous(new KsqlErrorMessage(-1, "error")))
            .once
          statement.execute("select * from test")
        }

        assertThrows[SQLException] {
          (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
            .once
          (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.erroneous(new KsqlErrorMessage(-1, "error")))
            .once
          statement.execute("select * from test")
        }

        assertThrows[SQLException] {
          (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
            .once
          (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
            .once
          statement.execute("select * from test")
        }

        val queryDesc = new QueryDescription(
          new EntityQueryId("id"),
          "select * from test;",
          List(
            new FieldInfo("field1", new SchemaInfo(SchemaInfo.Type.INTEGER, List.empty.asJava, None.orNull)),
            new FieldInfo("field2", new SchemaInfo(SchemaInfo.Type.BIGINT, List.empty.asJava, None.orNull)),
            new FieldInfo("field3", new SchemaInfo(SchemaInfo.Type.DOUBLE, List.empty.asJava, None.orNull)),
            new FieldInfo("field4", new SchemaInfo(SchemaInfo.Type.BOOLEAN, List.empty.asJava, None.orNull)),
            new FieldInfo("field5", new SchemaInfo(SchemaInfo.Type.STRING, List.empty.asJava, None.orNull)),
            new FieldInfo("field6", new SchemaInfo(SchemaInfo.Type.MAP, List.empty.asJava, None.orNull)),
            new FieldInfo("field7", new SchemaInfo(SchemaInfo.Type.ARRAY, List.empty.asJava, None.orNull)),
            new FieldInfo("field7", new SchemaInfo(SchemaInfo.Type.STRUCT, List.empty.asJava, None.orNull))

          ).asJava,
          Set("test").asJava,
          Set("sink1").asJava,
          "topologyTest",
          "executionPlanTest",
          Map.empty[String, AnyRef].asJava
        )

        val entityList = new KsqlEntityList
        entityList.add(new QueryDescriptionEntity("select * from test;", queryDesc))

        (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .once
        (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](entityList))
          .once
        statement.execute("select * from test") should be(true)

        (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .once
        (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](entityList))
          .once
        Option(statement.executeQuery("select * from test;")) should not be None

        statement.getMaxRows should be(0)
        statement.getResultSet shouldNot be(None.orNull)
        assertThrows[SQLException] {
          statement.setMaxRows(-1)
        }
        statement.setMaxRows(1)
        statement.getMaxRows should be(1)

        statement.getUpdateCount should be(-1)
        statement.getResultSetType should be(ResultSet.TYPE_FORWARD_ONLY)
        statement.getResultSetHoldability should be(ResultSet.HOLD_CURSORS_OVER_COMMIT)
        statement.getWarnings should be(None.orNull)

        assertThrows[SQLException] {
          (mockedKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
            .once
          val multipleResults = new KsqlEntityList
          multipleResults.add(new QueryDescriptionEntity("select * from test;", queryDesc))
          multipleResults.add(new QueryDescriptionEntity("select * from test;", queryDesc))
          (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](multipleResults))
            .once
          statement.execute("select * from test")
        }
        assertThrows[SQLException] {
          statement.getResultSet
        }
        statement.cancel()

        statement.isClosed should be(false)
        statement.close()
        statement.close()
        statement.isClosed should be(true)
        assertThrows[SQLException] {
          statement.executeQuery("select * from test;")
        }
      }

      "work when printing topics" in {
        (mockedKsqlRestClient.makePrintTopicRequest _).expects(*, *)
          .returns(RestResponse.successful[InputStream](new ByteArrayInputStream("test".getBytes)))
          .once
        Option(statement.executeQuery("print 'test'")) should not be None
        statement.getResultSet.next should be(true)
        statement.getResultSet.getString(1) should be("test")
      }

      "work when executing KSQL commands" in {
        import KsqlEntityHeaders._

        def validateCommand(entity: KsqlEntity, headers: List[HeaderField]): Unit = {
          val entityList = new KsqlEntityList
          entityList.add(entity)
          (mockedKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](entityList))
            .once
          statement.execute(entity.getStatementText) should be(true)
          statement.getResultSet.getMetaData.getColumnCount should be(headers.size)
          headers.zipWithIndex.map { case (c, index) =>
            statement.getResultSet.getMetaData.getColumnName(index + 1) should be(c.name)
            statement.getResultSet.getMetaData.getColumnLabel(index + 1).toUpperCase should be(c.name)
          }
        }

        val commandStatus = new CommandStatusEntity("REGISTER TOPIC TEST", CommandId.fromString("topic/1/create"),
          new CommandStatus(CommandStatus.Status.SUCCESS, "Success Message"), null)
        val executionPlan = new ExecutionPlan("DESCRIBE test")
        val functionDescriptionList = new FunctionDescriptionList("DESCRIBE FUNCTION test;",
          "TEST", "Description", "author", "version", "path",
          List(
            new FunctionInfo(List(new ArgumentInfo("arg1", "INT", "Description", false)).asJava, "BIGINT", "Description")
          ).asJava,
          FunctionType.scalar
        )
        val functionNameList = new FunctionNameList(
          "LIST FUNCTIONS;",
          List(new SimpleFunctionInfo("TESTFN", FunctionType.scalar)).asJava
        )
        val kafkaTopicsList = new KafkaTopicsList(
          "SHOW TOPICS;",
          List(new KafkaTopicInfo("test", false, List(Int.box(1)).asJava, 1, 1)).asJava
        )
        val ksqlTopicsList = new KsqlTopicsList(
          "SHOW TOPICS;",
          List(new KsqlTopicInfo("ksqltopic", "kafkatopic", Format.JSON)).asJava
        )
        val propertiesList = new PropertiesList(
          "list properties;",
          Map("key" -> "earliest").asJava,
          List.empty.asJava,
          List.empty.asJava
        )
        val queries = new Queries(
          "EXPLAIN select * from test",
          List(new RunningQuery("select * from test;", Set("Test").asJava, new EntityQueryId("id"))).asJava
        )
        val queryDescription = new QueryDescriptionEntity(
          "EXPLAIN select * from test;",
          new QueryDescription(
            new EntityQueryId("id"),
            "select * from test;",
            List(
              new FieldInfo("field1", new SchemaInfo(SchemaInfo.Type.INTEGER, List.empty.asJava, None.orNull)),
              new FieldInfo("field2", new SchemaInfo(SchemaInfo.Type.BIGINT, List.empty.asJava, None.orNull)),
              new FieldInfo("field3", new SchemaInfo(SchemaInfo.Type.DOUBLE, List.empty.asJava, None.orNull)),
              new FieldInfo("field4", new SchemaInfo(SchemaInfo.Type.BOOLEAN, List.empty.asJava, None.orNull)),
              new FieldInfo("field5", new SchemaInfo(SchemaInfo.Type.STRING, List.empty.asJava, None.orNull)),
              new FieldInfo("field6", new SchemaInfo(SchemaInfo.Type.MAP, List.empty.asJava, None.orNull)),
              new FieldInfo("field7", new SchemaInfo(SchemaInfo.Type.ARRAY, List.empty.asJava, None.orNull)),
              new FieldInfo("field7", new SchemaInfo(SchemaInfo.Type.STRUCT, List.empty.asJava, None.orNull))

            ).asJava,
            Set("test").asJava,
            Set("sink1").asJava,
            "topologyTest",
            "executionPlanTest",
            Map.empty[String, AnyRef].asJava
          )
        )
        val queryDescriptionList = new QueryDescriptionList(
          "EXPLAIN select * from test;",
          List(queryDescription.getQueryDescription).asJava
        )
        val schema = SchemaBuilder
          .struct
          .field("key", Schema.OPTIONAL_INT64_SCHEMA)
          .build()
        val sourceDescEntity = new SourceDescriptionEntity(
          "DESCRIBE TEST;",
          new SourceDescription(
            new KsqlStream("sqlExpression",
              "datasource",
              KsqlSchema.of(schema),
              KeyField.of("key", schema.field("key")),
              new LongColumnTimestampExtractionPolicy("timestamp"),
              new KsqlTopic("input", "input", new KsqlJsonSerdeFactory, true),
              new SerdeFactory[String] {
                override def create(): Serde[String] = Serdes.String()
              }),
            true,
            "JSON",
            List.empty.asJava,
            List.empty.asJava,
            None.orNull)
        )
        val sourceDescList = new SourceDescriptionList(
          "EXPLAIN select * from test;",
          List(sourceDescEntity.getSourceDescription).asJava
        )
        val streams = new StreamsList("SHOW STREAMS", List(new SourceInfo.Stream("TestStream", "TestTopic", "AVRO")).asJava)
        val tables = new TablesList("SHOW TABLES", List(new SourceInfo.Table("TestTable", "TestTopic", "JSON", false)).asJava)
        val topicDesc = new TopicDescription("DESCRIBE TEST", "TestTopic", "TestTopic", "AVRO", "schema")

        val commands = Seq(
          (commandStatus, commandStatusEntity),
          (executionPlan, executionPlanEntity),
          (functionDescriptionList, functionDescriptionListEntity),
          (functionNameList, functionNameListEntity),
          (kafkaTopicsList, kafkaTopicsListEntity),
          (ksqlTopicsList, ksqlTopicsListEntity),
          (propertiesList, propertiesListEntity),
          (queries, queriesEntity),
          (queryDescription, queryDescriptionEntity),
          (queryDescriptionList, queryDescriptionEntityList),
          (sourceDescEntity, sourceDescriptionEntity),
          (sourceDescList, sourceDescriptionEntityList),
          (streams, streamsListEntity),
          (tables, tablesListEntity),
          (topicDesc, topicDescriptionEntity)
        )
        commands.foreach(c => validateCommand(c._1, c._2))
      }
    }
  }

  "A StatementNotSupported" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new StatementNotSupported
        reflectMethods[StatementNotSupported](methods = Seq.empty, implemented = false, obj = resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }
    }
  }

}
