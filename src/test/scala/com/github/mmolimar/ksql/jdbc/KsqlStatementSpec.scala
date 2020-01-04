package com.github.mmolimar.ksql.jdbc

import java.io.{ByteArrayInputStream, InputStream}
import java.sql.{ResultSet, SQLException, SQLFeatureNotSupportedException}
import java.util.Collections.emptyList
import java.util.Optional

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.metastore.model.DataSource.DataSourceType
import io.confluent.ksql.name.ColumnName
import io.confluent.ksql.query.QueryId
import io.confluent.ksql.rest.client.{MockableKsqlRestClient, QueryStream, RestResponse}
import io.confluent.ksql.rest.entity.{ExecutionPlan, KafkaTopicsList, QueryDescriptionEntity, QueryDescriptionList, _}
import io.confluent.ksql.rest.util.EntityUtil
import io.confluent.ksql.schema.ksql.types.SqlTypes
import io.confluent.ksql.schema.ksql.{LogicalSchema, SqlBaseType => KsqlType}
import javax.ws.rs.core.Response
import org.apache.kafka.connect.runtime.rest.entities.{ConnectorInfo, ConnectorStateInfo, ConnectorType}
import org.eclipse.jetty.http.HttpStatus.Code
import org.scalamock.scalatest.MockFactory
import org.scalatest.OneInstancePerTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._


class KsqlStatementSpec extends AnyWordSpec with Matchers with MockFactory with OneInstancePerTest {

  "A KsqlStatement" when {

    val mockResponse = mock[Response]
    (mockResponse.getEntity _).expects.returns(mock[InputStream]).anyNumberOfTimes
    val mockKsqlRestClient = mock[MockableKsqlRestClient]
    val statement = new KsqlStatement(mockKsqlRestClient)

    "validating specs" should {

      "throw not supported exception if not supported" in {

        (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
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
          statement.execute("select * from test; select * from test;")
        }

        assertThrows[SQLException] {
          (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.erroneous(Code.INTERNAL_SERVER_ERROR, new KsqlErrorMessage(-1, "error")))
            .once
          statement.execute("select * from test")
        }

        assertThrows[SQLException] {
          (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
            .once
          (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.erroneous(Code.INTERNAL_SERVER_ERROR, new KsqlErrorMessage(-1, "error")))
            .once
          statement.execute("select * from test")
        }

        assertThrows[SQLException] {
          (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
            .once
          (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](Code.OK, new KsqlEntityList))
            .once
          statement.execute("select * from test")
        }

        val queryDesc = new QueryDescription(
          new QueryId("id"),
          "select * from test;",
          List(
            new FieldInfo("field1", new SchemaInfo(KsqlType.INTEGER, List.empty.asJava, None.orNull)),
            new FieldInfo("field2", new SchemaInfo(KsqlType.BIGINT, List.empty.asJava, None.orNull)),
            new FieldInfo("field3", new SchemaInfo(KsqlType.DOUBLE, List.empty.asJava, None.orNull)),
            new FieldInfo("field4", new SchemaInfo(KsqlType.DECIMAL, List.empty.asJava, None.orNull)),
            new FieldInfo("field5", new SchemaInfo(KsqlType.BOOLEAN, List.empty.asJava, None.orNull)),
            new FieldInfo("field6", new SchemaInfo(KsqlType.STRING, List.empty.asJava, None.orNull)),
            new FieldInfo("field7", new SchemaInfo(KsqlType.MAP, List.empty.asJava, None.orNull)),
            new FieldInfo("field8", new SchemaInfo(KsqlType.ARRAY, List.empty.asJava, None.orNull)),
            new FieldInfo("field9", new SchemaInfo(KsqlType.STRUCT, List.empty.asJava, None.orNull))

          ).asJava,
          Set("test").asJava,
          Set("sink1").asJava,
          "topologyTest",
          "executionPlanTest",
          Map.empty[String, AnyRef].asJava,
          Optional.empty[String]
        )

        val entityList = new KsqlEntityList
        entityList.add(new QueryDescriptionEntity("select * from test;", queryDesc))

        (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
          .once
        (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](Code.OK, entityList))
          .once
        statement.execute("select * from test") should be(true)

        (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
          .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
          .once
        (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](Code.OK, entityList))
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
          (mockKsqlRestClient.makeQueryRequest _).expects(*, *)
            .returns(RestResponse.successful[QueryStream](Code.OK, mockQueryStream(mockResponse)))
            .once
          val multipleResults = new KsqlEntityList
          multipleResults.add(new QueryDescriptionEntity("select * from test;", queryDesc))
          multipleResults.add(new QueryDescriptionEntity("select * from test;", queryDesc))
          (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](Code.ACCEPTED, multipleResults))
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
        (mockKsqlRestClient.makePrintTopicRequest _).expects(*, *)
          .returns(RestResponse.successful[InputStream](Code.OK, new ByteArrayInputStream("test".getBytes)))
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
          (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.successful[KsqlEntityList](Code.OK, entityList))
            .once
          statement.execute(entity.getStatementText) should be(true)
          statement.getResultSet.getMetaData.getColumnCount should be(headers.size)
          headers.zipWithIndex.map { case (c, index) =>
            statement.getResultSet.getMetaData.getColumnName(index + 1) should be(c.name)
            statement.getResultSet.getMetaData.getColumnLabel(index + 1).toUpperCase should be(c.name)
          }
        }

        val schema = LogicalSchema.builder.valueColumn(ColumnName.of("key"), SqlTypes.BIGINT).build
        val commandStatus = new CommandStatusEntity("CREATE STREAM TEST AS SELECT * FROM test",
          CommandId.fromString("topic/1/create"),
          new CommandStatus(CommandStatus.Status.SUCCESS, "Success Message"), null)
        val sourceDesc = new SourceDescription(
          "datasource",
          List(new RunningQuery("read query", Set("sink1").asJava, new QueryId("readId"))).asJava,
          List(new RunningQuery("read query", Set("sink1").asJava, new QueryId("readId"))).asJava,
          EntityUtil.buildSourceSchemaEntity(schema, false),
          DataSourceType.KTABLE.getKsqlType,
          "key",
          "2000-01-01",
          "stats",
          "errors",
          true,
          "avro",
          "kadka-topic",
          2,
          1
        )
        val connectorDesc = new ConnectorDescription(
          "DESCRIBE CONNECTOR `test-connector`",
          "test.Connector",
          new ConnectorStateInfo(
            "name",
            new ConnectorStateInfo.ConnectorState("state", "worker", "msg"),
            List(new ConnectorStateInfo.TaskState(0, "task", "worker", "task_msg")).asJava,
            ConnectorType.SOURCE
          ),
          List(sourceDesc).asJava,
          List("test-topic").asJava,
          List.empty.asJava)
        val connectorList = new ConnectorList(
          "SHOW CONNECTORS",
          List.empty.asJava,
          List(new SimpleConnectorInfo("testConnector", ConnectorType.SOURCE, "ConnectorClassname")).asJava
        )
        val createConnector = new CreateConnectorEntity(
          "CREATE SOURCE CONNECTOR test WITH ('prop1'='value')",
          new ConnectorInfo("connector",
            Map.empty[String, String].asJava,
            List.empty.asJava,
            ConnectorType.SOURCE)
        )
        val dropConnector = new DropConnectorEntity("DROP CONNECTOR `test-connector`", "TestConnector")
        val error = new ErrorEntity("SHOW CONNECTORS", "Error message")
        val executionPlan = new ExecutionPlan("DESCRIBE test")
        val functionDescriptionList = new FunctionDescriptionList("DESCRIBE FUNCTION test;",
          "TEST", "Description", "author", "version", "path",
          List(
            new FunctionInfo(List(new ArgumentInfo("arg1", "INT", "Description", false)).asJava, "BIGINT", "Description")
          ).asJava,
          FunctionType.SCALAR
        )
        val functionNameList = new FunctionNameList(
          "LIST FUNCTIONS;",
          List(new SimpleFunctionInfo("TESTFN", FunctionType.SCALAR)).asJava
        )
        val kafkaTopicsList = new KafkaTopicsList(
          "SHOW TOPICS;",
          List(new KafkaTopicInfo("test", List(Int.box(1)).asJava)).asJava
        )
        val kafkaTopicsListExt = new KafkaTopicsListExtended(
          "LIST TOPICS EXTENDED",
          List(new KafkaTopicInfoExtended("test", List(Int.box(1)).asJava, 5, 10)).asJava
        )
        val propertiesList = new PropertiesList(
          "list properties;",
          Map("key" -> "earliest").asJava,
          List.empty.asJava,
          List.empty.asJava
        )
        val queries = new Queries(
          "EXPLAIN select * from test",
          List(new RunningQuery("select * from test;", Set("Test").asJava, new QueryId("id"))).asJava
        )
        val queryDescription = new QueryDescriptionEntity(
          "EXPLAIN select * from test;",
          new QueryDescription(
            new QueryId("id"),
            "select * from test;",
            List(
              new FieldInfo("field1", new SchemaInfo(KsqlType.INTEGER, List.empty.asJava, None.orNull)),
              new FieldInfo("field2", new SchemaInfo(KsqlType.BIGINT, List.empty.asJava, None.orNull)),
              new FieldInfo("field3", new SchemaInfo(KsqlType.DOUBLE, List.empty.asJava, None.orNull)),
              new FieldInfo("field4", new SchemaInfo(KsqlType.DECIMAL, List.empty.asJava, None.orNull)),
              new FieldInfo("field5", new SchemaInfo(KsqlType.BOOLEAN, List.empty.asJava, None.orNull)),
              new FieldInfo("field6", new SchemaInfo(KsqlType.STRING, List.empty.asJava, None.orNull)),
              new FieldInfo("field7", new SchemaInfo(KsqlType.MAP, List.empty.asJava, None.orNull)),
              new FieldInfo("field8", new SchemaInfo(KsqlType.ARRAY, List.empty.asJava, None.orNull)),
              new FieldInfo("field9", new SchemaInfo(KsqlType.STRUCT, List.empty.asJava, None.orNull))

            ).asJava,
            Set("test").asJava,
            Set("sink1").asJava,
            "topologyTest",
            "executionPlanTest",
            Map.empty[String, AnyRef].asJava,
            Optional.empty[String]
          )
        )
        val queryDescriptionList = new QueryDescriptionList(
          "EXPLAIN select * from test;",
          List(queryDescription.getQueryDescription).asJava
        )
        val sourceDescEntity = new SourceDescriptionEntity(
          "DESCRIBE TEST;",
          sourceDesc,
          emptyList[KsqlWarning]
        )
        val sourceDescList = new SourceDescriptionList(
          "EXPLAIN select * from test;",
          List(sourceDescEntity.getSourceDescription).asJava,
          emptyList[KsqlWarning]
        )
        val streamsList = new StreamsList("SHOW STREAMS", List(new SourceInfo.Stream("TestStream", "TestTopic", "AVRO")).asJava)
        val tablesList = new TablesList("SHOW TABLES", List(new SourceInfo.Table("TestTable", "TestTopic", "JSON", false)).asJava)
        val topicDesc = new TopicDescription("DESCRIBE TEST", "TestTopic", "TestTopic", "AVRO", "schema")
        val types = new TypeList(
          "SHOW TYPES",
          Map("typeTest" -> new SchemaInfo(KsqlType.ARRAY, List.empty.asJava, None.orNull)).asJava
        )

        val commands = Seq(
          (commandStatus, commandStatusEntity),
          (connectorDesc, connectorDescriptionEntity),
          (connectorList, connectorListEntity),
          (createConnector, createConnectorEntity),
          (dropConnector, dropConnectorEntity),
          (error, errorEntity),
          (executionPlan, executionPlanEntity),
          (functionDescriptionList, functionDescriptionListEntity),
          (functionNameList, functionNameListEntity),
          (kafkaTopicsList, kafkaTopicsListEntity),
          (kafkaTopicsListExt, kafkaTopicsListExtendedEntity),
          (propertiesList, propertiesListEntity),
          (queries, queriesEntity),
          (queryDescription, queryDescriptionEntity),
          (queryDescriptionList, queryDescriptionEntityList),
          (sourceDescEntity, sourceDescriptionEntity),
          (sourceDescList, sourceDescriptionEntityList),
          (streamsList, streamsListEntity),
          (tablesList, tablesListEntity),
          (topicDesc, topicDescriptionEntity),
          (types, typesListEntity),
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
