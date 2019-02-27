package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, DriverManager, ResultSet, SQLException, Types}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.github.mmolimar.ksql.jdbc.KsqlEntityHeaders._
import com.github.mmolimar.ksql.jdbc.embedded.{EmbeddedKafkaCluster, EmbeddedKsqlEngine, EmbeddedZookeeperServer}
import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest._

class KsqlDriverIntegrationTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val zkServer = new EmbeddedZookeeperServer
  val kafkaCluster = new EmbeddedKafkaCluster(zkServer.getConnection)
  val ksqlEngine = new EmbeddedKsqlEngine(kafkaCluster.getBrokerList)

  lazy val kafkaProducer = TestUtils.buildProducer(kafkaCluster.getBrokerList)

  val ksqlUrl = s"jdbc:ksql://localhost:${ksqlEngine.getPort}?timeout=20000"
  var ksqlConnection: Connection = _
  val topic = TestUtils.randomString()

  val stop = new AtomicBoolean(false)
  val producerThread = new BackgroundOps(stop, () => produceMessages)

  "A KsqlConnection" when {

    "managing a TABLE" should {

      val maxRecords = 5
      val table = TestUtils.randomString()

      "create the table properly" in {
        val resultSet = createTestTableOrStream(table)
        resultSet.next should be(true)
        resultSet.getString(commandStatusEntity(0).name) should be("TABLE")
        resultSet.getString(commandStatusEntity(1).name) should be(table.toUpperCase)
        resultSet.getString(commandStatusEntity(2).name) should be("CREATE")
        resultSet.getString(commandStatusEntity(3).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(4).name) should be("Table created")
        resultSet.next should be(false)
        resultSet.close
      }

      "list the table already created" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SHOW TABLES")
        resultSet.next should be(true)
        resultSet.getString(tablesListEntity(0).name) should be(table.toUpperCase)
        resultSet.getString(tablesListEntity(1).name) should be(topic)
        resultSet.getString(tablesListEntity(2).name) should be("JSON")
        resultSet.getBoolean(tablesListEntity(3).name) should be(false)
        resultSet.next should be(false)
        resultSet.close
      }

      "be able to get the execution plan for a query in a table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"EXPLAIN SELECT * FROM $table")
        resultSet.next should be(true)
        resultSet.getString(queryDescriptionEntity(1).name) should be("ROWTIME, ROWKEY, FIELD1, FIELD2, FIELD3")
        resultSet.getString(queryDescriptionEntity(2).name) should be(table.toUpperCase)
        resultSet.next should be(false)
        resultSet.close
      }

      "be able to query all fields in the table" in {
        var counter = 0
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(maxRecords)
        val resultSet = statement.executeQuery(s"SELECT * FROM $table")
        while (resultSet.next) {
          resultSet.getLong(1) should not be (-1)
          Option(resultSet.getString(2)) should not be (None)
          resultSet.getInt(3) should be(123)
          resultSet.getDouble(4) should be(45.4)
          resultSet.getString(5) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(6)
          }
          counter += 1
        }
        counter should be(maxRecords)

        resultSet.close
        statement.close

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
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SELECT FIELD3 FROM $table LIMIT $maxRecords")
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
        resultSet.getString(sourceDescriptionEntity(1).name) should be(table.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(3).name) should be("TABLE")
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close
      }

      "drop the table" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP TABLE $table")
        resultSet.next should be(true)
        resultSet.getString(commandStatusEntity(0).name) should be("TABLE")
        resultSet.getString(commandStatusEntity(1).name) should be(table.toUpperCase)
        resultSet.getString(commandStatusEntity(2).name) should be("DROP")
        resultSet.getString(commandStatusEntity(3).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(4).name) should be(s"Source ${table.toUpperCase} was dropped. ")
        resultSet.next should be(false)
        resultSet.close
      }
    }

    "managing a STREAM" should {

      val maxRecords = 5
      val stream = TestUtils.randomString()

      "create the stream properly" in {
        val resultSet = createTestTableOrStream(stream, true)
        resultSet.next should be(true)
        resultSet.getString(commandStatusEntity(0).name) should be("STREAM")
        resultSet.getString(commandStatusEntity(1).name) should be(stream.toUpperCase)
        resultSet.getString(commandStatusEntity(2).name) should be("CREATE")
        resultSet.getString(commandStatusEntity(3).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(4).name) should be("Stream created")
        resultSet.next should be(false)
        resultSet.close
      }

      "list the stream already created" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SHOW STREAMS")
        resultSet.next should be(true)
        resultSet.getString(streamsListEntity(0).name) should be(stream.toUpperCase)
        resultSet.getString(streamsListEntity(1).name) should be(topic)
        resultSet.getString(streamsListEntity(2).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close
      }

      "be able to get the execution plan for a query in a stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"EXPLAIN SELECT * FROM $stream")
        resultSet.next should be(true)
        resultSet.getString(queryDescriptionEntity(1).name) should be("ROWTIME, ROWKEY, FIELD1, FIELD2, FIELD3")
        resultSet.getString(queryDescriptionEntity(2).name) should be(stream.toUpperCase)
        resultSet.next should be(false)
        resultSet.close
      }

      "be able to query all fields in the stream" in {
        var counter = 0
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(maxRecords)
        val resultSet = statement.executeQuery(s"SELECT * FROM $stream")
        while (resultSet.next) {
          resultSet.getLong(1) should not be (-1)
          Option(resultSet.getString(2)) should not be (None)
          resultSet.getInt(3) should be(123)
          resultSet.getDouble(4) should be(45.4)
          resultSet.getString(5) should be("lorem ipsum")
          assertThrows[SQLException] {
            resultSet.getString(6)
          }
          counter += 1
        }
        counter should be(maxRecords)

        resultSet.close
        statement.close

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
        val resultSet = ksqlConnection.createStatement.executeQuery(s"SELECT FIELD3 FROM $stream LIMIT $maxRecords")
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
        resultSet.getString(sourceDescriptionEntity(1).name) should be(stream.toUpperCase)
        resultSet.getString(sourceDescriptionEntity(2).name) should be(topic)
        resultSet.getString(sourceDescriptionEntity(3).name) should be("STREAM")
        resultSet.getString(sourceDescriptionEntity(4).name) should be("JSON")
        resultSet.next should be(false)
        resultSet.close
      }

      "drop the stream" in {
        val resultSet = ksqlConnection.createStatement.executeQuery(s"DROP STREAM $stream")
        resultSet.next should be(true)
        resultSet.getString(commandStatusEntity(0).name) should be("STREAM")
        resultSet.getString(commandStatusEntity(1).name) should be(stream.toUpperCase)
        resultSet.getString(commandStatusEntity(2).name) should be("DROP")
        resultSet.getString(commandStatusEntity(3).name) should be("SUCCESS")
        resultSet.getString(commandStatusEntity(4).name) should be(s"Source ${stream.toUpperCase} was dropped. ")
        resultSet.next should be(false)
        resultSet.close
      }
    }

    "printing a Kafka topic" should {

      "show the content of that topic" in {
        val statement = ksqlConnection.createStatement
        statement.setMaxRows(3)
        val resultSet = statement.executeQuery(s"PRINT '$topic'")
        resultSet.next should be(true)
        resultSet.getString(printTopic(0).name) should be("Format:STRING")
        resultSet.next should be(true)
        resultSet.next should be(true)
        resultSet.next should be(false)
        resultSet.close
        statement.close
      }
    }
  }

  private def produceMessages: Unit = {
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

  private def createTestTableOrStream(str: String, isStream: Boolean = false): ResultSet = {
    ksqlConnection.createStatement.executeQuery(s"CREATE ${if (isStream) "STREAM" else "TABLE"} $str " +
      s"(FIELD1 INT, FIELD2 DOUBLE, FIELD3 VARCHAR) " +
      s"WITH (KAFKA_TOPIC='$topic', VALUE_FORMAT='JSON', KEY='FIELD1');")
  }

  override def beforeAll = {
    DriverManager.registerDriver(new KsqlDriver);

    zkServer.startup
    TestUtils.waitTillAvailable("localhost", zkServer.getPort, 5000)

    kafkaCluster.startup
    kafkaCluster.getPorts.foreach { port =>
      TestUtils.waitTillAvailable("localhost", port, 5000)
    }

    kafkaCluster.createTopic(topic)
    kafkaCluster.existTopic(topic) should be(true)
    producerThread.start

    ksqlEngine.startup
    TestUtils.waitTillAvailable("localhost", ksqlEngine.getPort, 5000)

    ksqlConnection = DriverManager.getConnection(ksqlUrl)

  }

  override def afterAll = {
    info(s"Produced ${producerThread.getNumExecs} messages")
    stop.set(true)
    TestUtils.swallow(producerThread.interrupt)

    TestUtils.swallow(ksqlConnection.close)
    ksqlEngine.shutdown
    TestUtils.swallow(kafkaProducer.close)

    kafkaCluster.shutdown
    zkServer.shutdown
  }

}

class BackgroundOps(stop: AtomicBoolean, exec: () => Unit) extends Thread {
  private var count = 0L

  override def run = {
    while (!stop.get) {
      exec()
      this.count += 1
    }
  }

  def getNumExecs = this.count
}

