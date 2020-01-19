package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, DriverManager, ResultSet, SQLException, Types}
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.github.mmolimar.ksql.jdbc.KsqlEntityHeaders._
import com.github.mmolimar.ksql.jdbc.embedded._
import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import io.confluent.ksql.util.KsqlConstants.ESCAPE
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KsqlDriverIntegrationTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val zkServer = new EmbeddedZookeeperServer
  val kafkaCluster = new EmbeddedKafkaCluster(zkConnection = zkServer.getConnection)
  val kafkaConnect = new EmbeddedKafkaConnect(brokerList = kafkaCluster.getBrokerList)
  val ksqlEngine = new EmbeddedKsqlEngine(brokerList = kafkaCluster.getBrokerList, connectUrl = kafkaConnect.getUrl)

  lazy val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = TestUtils.buildProducer(kafkaCluster.getBrokerList)

  val ksqlUrl = s"jdbc:ksql://localhost:${ksqlEngine.getPort}?timeout=20000"
  var ksqlConnection: Connection = _
  val topic: String = "test-topic"

  val stop = new AtomicBoolean(false)
  val producerThread = new BackgroundOps(stop, () => produceMessages())

  "A KsqlConnection" when {

    "managing a TABLE" should {

      val maxRecords = 5
      val table = "TEST_TABLE"

      "create the table properly" in {
        val resultSet = createTestTableOrStream(table)
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(0L)
        resultSet.getString(commandStatusEntity(1).name) should be("TABLE")
        resultSet.getString(commandStatusEntity(2).name) should be(table.toUpperCase.escape)
        resultSet.getString(commandStatusEntity(3).name) should be("CREATE")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be("Table created")
        resultSet.next should be(false)
        resultSet.close()
      }

      "insert records into the table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"INSERT INTO $table (FIELD1, FIELD2, FIELD3) VALUES (123, 45.4, 'lorem ipsum')")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list the table already created" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SHOW TABLES")
        resultSet.next should be(true)
        resultSet.getString(tablesListEntity.head.name) should be(table.toUpperCase)
        resultSet.getString(tablesListEntity(1).name) should be(topic)
        resultSet.getString(tablesListEntity(2).name) should be("JSON")
        resultSet.getBoolean(tablesListEntity(3).name) should be(false)
        resultSet.next should be(false)
        resultSet.close()
      }

      "be able to get the execution plan for a query in a table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"EXPLAIN SELECT * FROM $table")
        resultSet.next should be(true)
        resultSet.getString(queryDescriptionEntity(1).name) should be("ROWKEY, FIELD1, FIELD2, FIELD3")
        resultSet.getString(queryDescriptionEntity(2).name) should be(table.toUpperCase)
        resultSet.next should be(false)
        resultSet.close()
      }

      "be able to query all fields in the table" in {
        var counter = 0
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(maxRecords)
        statement.getMoreResults(1) should be(false)
        val resultSet = statement.executeQuery(s"SELECT * FROM $table EMIT CHANGES")
        statement.getMoreResults(1) should be(true)
        while (resultSet.next) {
          resultSet.getLong(1) should not be (-1)
          Option(resultSet.getString(2)) should not be None
          resultSet.getInt(3) should be(123)
          resultSet.getDouble(4) should be(45.4)
          resultSet.getString(5) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(6)
          }
          counter += 1
        }
        counter should be(maxRecords)
        statement.getMoreResults() should be(false)

        resultSet.close()
        statement.close()

        val metadata = resultSet.getMetaData
        metadata.getColumnCount should be(5)
        metadata.getColumnName(1) should be("ROWTIME")
        metadata.getColumnName(2) should be("ROWKEY")
        metadata.getColumnName(3) should be("FIELD1")
        metadata.getColumnName(4) should be("FIELD2")
        metadata.getColumnName(5) should be("FIELD3")
      }

      "be able to query one field in the table and get its metadata" in {
        var counter = 0
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SELECT FIELD3 FROM $table EMIT CHANGES LIMIT $maxRecords")
        while (resultSet.next) {
          resultSet.getString(1) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(2)
          }
          counter += 1
        }
        counter should be(maxRecords)

        val metadata = resultSet.getMetaData
        metadata.getColumnCount should be(1)
        metadata.getColumnName(1) should be("FIELD3")

      }

      "be able to get the metadata for this table" in {
        var resultSet = ksqlConnection.getMetaData.getTables("", "", table, TableTypes.tableTypes.map(_.name).toArray)
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(table.toUpperCase)
          resultSet.getString("TABLE_TYPE") should be(TableTypes.TABLE.name)
          resultSet.getString("TYPE_SCHEM") should be("JSON")
          resultSet.getString("REMARKS") should be(s"Topic: $topic. Windowed: false")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", table, "")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(table.toUpperCase)
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", table, "FIELD2")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(table.toUpperCase)
          resultSet.getString("COLUMN_NAME") should be("FIELD2")
          resultSet.getInt("DATA_TYPE") should be(Types.DOUBLE)
          resultSet.getString("TYPE_NAME") should be("DOUBLE")
          resultSet.getString("IS_AUTOINCREMENT") should be("NO")
          resultSet.getString("IS_GENERATEDCOLUMN") should be("NO")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", table, "_ID")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(table.toUpperCase)
          resultSet.getString("COLUMN_NAME") should be("_ID")
          resultSet.getInt("DATA_TYPE") should be(Types.BIGINT)
          resultSet.getString("TYPE_NAME") should be("BIGINT")
          resultSet.getString("IS_AUTOINCREMENT") should be("YES")
          resultSet.getString("IS_GENERATEDCOLUMN") should be("YES")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }
      }

      "describe the table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DESCRIBE $table")
        resultSet.next should be(true)
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.getString(sourceDescriptionEntity.head.name) should be("FIELD1")
        resultSet.getString(sourceDescriptionEntity(1).name) should be(table.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be("TABLE")
        resultSet.getString(sourceDescriptionEntity(3).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close()
      }

      "describe extended the table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DESCRIBE EXTENDED $table")
        resultSet.next should be(true)
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.getString(sourceDescriptionEntity.head.name) should be("FIELD1")
        resultSet.getString(sourceDescriptionEntity(1).name) should be(table.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be("TABLE")
        resultSet.getString(sourceDescriptionEntity(3).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close()
      }

      "drop the table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP TABLE $table")
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(1L)
        resultSet.getString(commandStatusEntity(1).name) should be("TABLE")
        resultSet.getString(commandStatusEntity(2).name) should be(table.toUpperCase)
        resultSet.getString(commandStatusEntity(3).name) should be("DROP")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be(s"Source ${table.toUpperCase.escape} (topic: $topic) was dropped.")
        resultSet.next should be(false)
        resultSet.close()
      }
    }

    "managing a STREAM" should {

      val maxRecords = 5
      val stream = "TEST_STREAM"

      "create the stream properly" in {
        val resultSet = createTestTableOrStream(str = stream, isStream = true)
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(2L)
        resultSet.getString(commandStatusEntity(1).name) should be("STREAM")
        resultSet.getString(commandStatusEntity(2).name) should be(stream.toUpperCase.escape)
        resultSet.getString(commandStatusEntity(3).name) should be("CREATE")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be("Stream created")
        resultSet.next should be(false)
        resultSet.close()
      }

      "insert records into the stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"INSERT INTO $stream (FIELD1, FIELD2, FIELD3) VALUES (123, 45.4, 'lorem ipsum')")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list the stream already created" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SHOW STREAMS")
        resultSet.next should be(true)
        resultSet.getString(streamsListEntity.head.name) should be(stream.toUpperCase)
        resultSet.getString(streamsListEntity(1).name) should be(topic)
        resultSet.getString(streamsListEntity(2).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close()
      }

      "be able to get the execution plan for a query in a stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"EXPLAIN SELECT * FROM $stream")
        resultSet.next should be(true)
        resultSet.getString(queryDescriptionEntity(1).name) should be("ROWKEY, FIELD1, FIELD2, FIELD3")
        resultSet.getString(queryDescriptionEntity(2).name) should be(stream.toUpperCase)
        resultSet.next should be(false)
        resultSet.close()
      }

      "be able to query all fields in the stream" in {
        var counter = 0
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(maxRecords)
        statement.getMoreResults(1) should be(false)
        val resultSet = statement.executeQuery(s"SELECT * FROM $stream EMIT CHANGES")
        statement.getMoreResults(1) should be(true)
        while (resultSet.next) {
          resultSet.getLong(1) should not be (-1)
          Option(resultSet.getString(2)) should not be None
          resultSet.getInt(3) should be(123)
          resultSet.getDouble(4) should be(45.4)
          resultSet.getString(5) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(6)
          }
          counter += 1
        }
        counter should be(maxRecords)
        statement.getMoreResults(1) should be(false)

        resultSet.close()
        statement.close()

        val metadata = resultSet.getMetaData
        metadata.getColumnCount should be(5)
        metadata.getColumnName(1) should be("ROWTIME")
        metadata.getColumnName(2) should be("ROWKEY")
        metadata.getColumnName(3) should be("FIELD1")
        metadata.getColumnName(4) should be("FIELD2")
        metadata.getColumnName(5) should be("FIELD3")
      }

      "be able to query one field in the stream" in {
        var counter = 0
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SELECT FIELD3 FROM $stream EMIT CHANGES LIMIT $maxRecords")
        while (resultSet.next) {
          resultSet.getString(1) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(2)
          }
          counter += 1
        }
        counter should be(maxRecords)

        val metadata = resultSet.getMetaData
        metadata.getColumnCount should be(1)
        metadata.getColumnName(1) should be("FIELD3")
      }

      "be able to get the metadata for this stream" in {
        var resultSet = ksqlConnection.getMetaData.getTables("", "", stream, TableTypes.tableTypes.map(_.name).toArray)
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(stream.toUpperCase)
          resultSet.getString("TABLE_TYPE") should be(TableTypes.STREAM.name)
          resultSet.getString("TYPE_SCHEM") should be("JSON")
          resultSet.getString("REMARKS") should be(s"Topic: $topic")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", stream, "")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(stream.toUpperCase)
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", stream, "FIELD2")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(stream.toUpperCase)
          resultSet.getString("COLUMN_NAME") should be("FIELD2")
          resultSet.getInt("DATA_TYPE") should be(Types.DOUBLE)
          resultSet.getString("TYPE_NAME") should be("DOUBLE")
          resultSet.getString("IS_AUTOINCREMENT") should be("NO")
          resultSet.getString("IS_GENERATEDCOLUMN") should be("NO")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }

        resultSet = ksqlConnection.getMetaData.getColumns("", "", stream, "_ID")
        while (resultSet.next) {
          resultSet.getString("TABLE_NAME") should be(stream.toUpperCase)
          resultSet.getString("COLUMN_NAME") should be("_ID")
          resultSet.getInt("DATA_TYPE") should be(Types.BIGINT)
          resultSet.getString("TYPE_NAME") should be("BIGINT")
          resultSet.getString("IS_AUTOINCREMENT") should be("YES")
          resultSet.getString("IS_GENERATEDCOLUMN") should be("YES")
          assertThrows[SQLException] {
            resultSet.getString("UNKNOWN")
          }
        }
      }

      "describe the stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DESCRIBE $stream")
        resultSet.next should be(true)
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.getString(sourceDescriptionEntity.head.name) should be("FIELD1")
        resultSet.getString(sourceDescriptionEntity(1).name) should be(stream.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be("STREAM")
        resultSet.getString(sourceDescriptionEntity(3).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.next should be(false)
        resultSet.close()
      }

      "describe extended the stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DESCRIBE EXTENDED $stream")
        resultSet.next should be(true)
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.getString(sourceDescriptionEntity.head.name) should be("FIELD1")
        resultSet.getString(sourceDescriptionEntity(1).name) should be(stream.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be("STREAM")
        resultSet.getString(sourceDescriptionEntity(3).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.getMetaData.getColumnCount should be(sourceDescriptionEntity.size)
        resultSet.next should be(false)
        resultSet.close()
      }

      "drop the stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP STREAM $stream")
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(3L)
        resultSet.getString(commandStatusEntity(1).name) should be("STREAM")
        resultSet.getString(commandStatusEntity(2).name) should be(stream.toUpperCase)
        resultSet.getString(commandStatusEntity(3).name) should be("DROP")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be(s"Source ${stream.toUpperCase.escape} (topic: $topic) was dropped.")
        resultSet.next should be(false)
        resultSet.close()
      }
    }

    "managing a CONNECTOR" should {

      val connectorName = TestUtils.randomString()
      val connectorClass = "org.apache.kafka.connect.tools.MockSourceConnector"

      "create the connector properly" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"CREATE SOURCE CONNECTOR `$connectorName` " +
          s"""WITH("connector.class"='$connectorClass')""")
        resultSet.next should be(true)
        resultSet.getString(createConnectorEntity.head.name) should be(connectorName)
        resultSet.getString(createConnectorEntity(1).name) should be("SOURCE")
        resultSet.getString(createConnectorEntity(2).name) should be(s"[0]-$connectorName")
        resultSet.getString(createConnectorEntity(3).name) should be(s"connector.class -> $connectorClass\n" +
          s"name -> $connectorName")
        resultSet.next should be(false)
        resultSet.close()
      }

      "describe the connector" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DESCRIBE CONNECTOR `$connectorName`")
        resultSet.next should be(true)
        resultSet.getString(connectorDescriptionEntity.head.name) should be(connectorClass)
        resultSet.getString(connectorDescriptionEntity(1).name) should be(connectorName)
        resultSet.getString(connectorDescriptionEntity(2).name) should be("SOURCE")
        resultSet.getString(connectorDescriptionEntity(3).name) should be("RUNNING")
        resultSet.getString(connectorDescriptionEntity(4).name) should be(None.orNull)
        resultSet.getString(connectorDescriptionEntity(5).name) should be(kafkaConnect.getWorker)
        resultSet.getString(connectorDescriptionEntity(6).name) should be(s"0-RUNNING-${kafkaConnect.getWorker}: ")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list all connectors" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SHOW CONNECTORS")
        resultSet.next should be(true)
        resultSet.getString(connectorListEntity.head.name) should be(connectorName)
        resultSet.getString(connectorListEntity(1).name) should be("SOURCE")
        resultSet.getString(connectorListEntity(2).name) should be(connectorClass)
        resultSet.next should be(false)
        resultSet.close()
      }

      "drop the connector" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP CONNECTOR `$connectorName`")
        resultSet.next should be(true)
        resultSet.getString(dropConnectorEntity.head.name) should be(connectorName)
        resultSet.next should be(false)
        resultSet.close()
      }
    }

    "managing a TYPE" should {

      val testType = "TEST"

      "create a specified type" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"CREATE TYPE $testType AS STRUCT<F1 VARCHAR, F2 VARCHAR>")
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(4L)
        resultSet.getString(commandStatusEntity(1).name) should be("TYPE")
        resultSet.getString(commandStatusEntity(2).name) should be(testType)
        resultSet.getString(commandStatusEntity(3).name) should be("CREATE")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be(s"Registered custom type with name '$testType' and SQL type STRUCT<`F1` STRING, `F2` STRING>")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list all defined types" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SHOW TYPES")
        resultSet.next should be(true)
        resultSet.getString(typesListEntity.head.name) should be(testType)
        resultSet.getString(typesListEntity(1).name) should be("STRUCT")
        resultSet.next should be(false)
        resultSet.close()
      }

      "drop the type" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP TYPE $testType")
        resultSet.next should be(true)
        resultSet.getLong(commandStatusEntity.head.name) should be(5L)
        resultSet.getString(commandStatusEntity(1).name) should be("TYPE")
        resultSet.getString(commandStatusEntity(2).name) should be(testType)
        resultSet.getString(commandStatusEntity(3).name) should be("DROP")
        resultSet.getString(commandStatusEntity(4).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(5).name) should be(s"Dropped type '$testType'")
        resultSet.next should be(false)
        resultSet.close()
      }

    }

    "getting info from topics" should {

      "print a topic" in {
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(3)
        statement.getMoreResults(1) should be(false)
        val resultSet = statement.executeQuery(s"PRINT '$topic'")
        statement.getMoreResults(1) should be(true)
        resultSet.next should be(true)
        resultSet.getString(printTopic.head.name) should be("Format:STRING")
        resultSet.next should be(true)
        resultSet.next should be(true)
        resultSet.next should be(true)
        resultSet.next should be(false)
        statement.getMoreResults() should be(false)
        resultSet.close()
        statement.close()
      }

      "list topics" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SHOW TOPICS")
        resultSet.next should be(true)
        resultSet.getString(kafkaTopicsListEntity.head.name) should be(topic)
        resultSet.getString(kafkaTopicsListEntity(1).name) should be("1")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list topics extended" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SHOW TOPICS EXTENDED")
        resultSet.next should be(true)
        resultSet.getString(kafkaTopicsListExtendedEntity.head.name) should be(topic)
        resultSet.getString(kafkaTopicsListExtendedEntity(1).name) should be("1")
        resultSet.getString(kafkaTopicsListExtendedEntity(2).name) should be("2")
        resultSet.getString(kafkaTopicsListExtendedEntity(3).name) should be("2")
        resultSet.next should be(false)
        resultSet.close()
      }
    }

    "setting properties" should {

      "set client info properties" in {
        val props = new Properties()
        props.setProperty("group.id", "test-group")
        props.setProperty("commit.interval.ms", "0")
        ksqlConnection.setClientInfo(props)

        assertThrows[SQLException] {
          ksqlConnection.setClientInfo("invalid.property", "test")
        }
      }

      "set a property" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SET 'group.id' = 'test-group'")
        resultSet.next should be(false)
        resultSet.close()
      }

      "unset a property" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("UNSET 'group.id'")
        resultSet.next should be(false)
        resultSet.close()
      }

      "list properties" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SHOW PROPERTIES")
        resultSet.next should be(true)
        resultSet.getString(propertiesListEntity.head.name) should not be None.orNull
        resultSet.getString(propertiesListEntity(1).name) should not be None.orNull
        while(resultSet.next()) {}
        resultSet.close()
      }
    }

    "querying functions" should {

      "list functions" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("SHOW FUNCTIONS")
        resultSet.next should be(true)
        resultSet.getString(functionNameListEntity.head.name) should not be None.orNull
        resultSet.getString(functionNameListEntity(1).name) should not be None.orNull
        while(resultSet.next()) {}
        resultSet.close()
      }

      "describe a function" in {
        val resultSet = ksqlConnection.createStatement.executeQuery("DESCRIBE FUNCTION UNIX_DATE")
        resultSet.next should be(true)
        resultSet.getString(functionDescriptionListEntity.head.name) should be("UNIX_DATE")
        resultSet.getString(functionDescriptionListEntity(1).name) should be("SCALAR")
        resultSet.getString(functionDescriptionListEntity(2).name) should be("Gets an integer representing days since epoch.")
        resultSet.getString(functionDescriptionListEntity(3).name) should be("internal")
        resultSet.getString(functionDescriptionListEntity(4).name) should be("")
        resultSet.getString(functionDescriptionListEntity(5).name) should be("")
        resultSet.getString(functionDescriptionListEntity(6).name) should be("Gets an integer representing days since epoch.")
        resultSet.getString(functionDescriptionListEntity(7).name) should be("INT")
        resultSet.getString(functionDescriptionListEntity(8).name) should be("")
        resultSet.next should be(false)
        resultSet.close()
      }
    }
  }

  private def produceMessages(): Unit = {
    val key = TestUtils.randomString().getBytes
    val value =
      """
        |{
        |	"FIELD1": 123,
        |	"FIELD2": 45.4,
        |	"FIELD3": "lorem ipsum"
        |}
      """.stripMargin.getBytes
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
    kafkaProducer.send(record).get(10000, TimeUnit.MILLISECONDS)
    Thread.sleep(100)
  }

  private implicit class Escape(str: String) {
    def escape: String = s"$ESCAPE$str$ESCAPE"
  }

  private def createTestTableOrStream(str: String, isStream: Boolean = false): ResultSet = {
    ksqlConnection.createStatement.executeQuery(s"CREATE ${if (isStream) "STREAM" else "TABLE"} $str " +
      s"(FIELD1 INT, FIELD2 DOUBLE, FIELD3 VARCHAR) " +
      s"WITH (KAFKA_TOPIC='$topic', VALUE_FORMAT='JSON', KEY='FIELD1');")
  }

  override def beforeAll(): Unit = {
    DriverManager.registerDriver(new KsqlDriver)

    zkServer.startup()
    TestUtils.waitTillAvailable("localhost", zkServer.getPort, 5000)

    kafkaCluster.startup()
    kafkaCluster.getPorts.foreach { port =>
      TestUtils.waitTillAvailable("localhost", port, 5000)
    }

    kafkaCluster.createTopic(topic)
    kafkaCluster.existTopic(topic) should be(true)
    producerThread.start()

    kafkaConnect.startup()
    TestUtils.waitTillAvailable("localhost", kafkaConnect.getPort, 5000)

    ksqlEngine.startup()
    TestUtils.waitTillAvailable("localhost", ksqlEngine.getPort, 5000)

    ksqlConnection = DriverManager.getConnection(ksqlUrl)
  }

  override def afterAll(): Unit = {
    info(s"Produced ${producerThread.getNumExecs} messages")
    stop.set(true)
    TestUtils.swallow(producerThread.interrupt())

    TestUtils.swallow(ksqlConnection.close())
    ksqlEngine.shutdown()

    kafkaConnect.shutdown()

    TestUtils.swallow(kafkaProducer.close())
    kafkaCluster.shutdown()
    zkServer.shutdown()
  }

}

class BackgroundOps(stop: AtomicBoolean, exec: () => Unit) extends Thread {
  private var count = 0L

  override def run(): Unit = {
    while (!stop.get) {
      exec()
      this.count += 1
    }
  }

  def getNumExecs: Long = this.count
}
