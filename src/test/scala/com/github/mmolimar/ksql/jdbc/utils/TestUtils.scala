package com.github.mmolimar.ksql.jdbc.utils

import java.io.{File, FileNotFoundException, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.channels.ServerSocketChannel
import java.util
import java.util.{Properties, Random, UUID}

import _root_.io.confluent.ksql.rest.client.KsqlRestClient
import javax.ws.rs.client.Client
import javax.ws.rs.core.Response
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.Time

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, _}

object TestUtils extends Logging {

  private val RANDOM: Random = new Random

  def constructTempDir(dirPrefix: String): File = {
    val file: File = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000))
    if (!file.mkdirs) throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath)
    file.deleteOnExit()
    file
  }

  def getAvailablePort: Int = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.getLocalPort
    } catch {
      case e: IOException => throw new IllegalStateException("Cannot find available port: " + e.getMessage, e)
    }
    finally socket.close()
  }

  def waitTillAvailable(host: String, port: Int, maxWaitMs: Int): Unit = {
    val defaultWait: Int = 100
    var currentWait: Int = 0
    try
        while (isPortAvailable(host, port) && currentWait < maxWaitMs) {
          Thread.sleep(defaultWait)
          currentWait += defaultWait
        }

    catch {
      case ie: InterruptedException => throw new RuntimeException(ie)
    }
  }

  def isPortAvailable(host: String, port: Int): Boolean = {
    var ss: ServerSocketChannel = null
    try {
      ss = ServerSocketChannel.open
      ss.socket.setReuseAddress(false)
      ss.socket.bind(new InetSocketAddress(host, port))
      true

    } catch {
      case ioe: IOException => false
    }
    finally if (Option(ss).isDefined) ss.close()
  }

  def buildProducer(brokerList: String, compression: String = "none"): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0") //ensure writes are synchronous
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
  }

  def buildConsumer(brokerList: String, groupId: String = "test-group"): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-client-" + UUID.randomUUID)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching

    new KafkaConsumer(props, new ByteArrayDeserializer, new ByteArrayDeserializer)
  }

  def buildAdminClient(brokerList: String): AdminClient = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    AdminClient.create(config)
  }

  def buildZkClient(zkConnection: String): KafkaZkClient =
    KafkaZkClient(connectString = zkConnection, isSecure = false, sessionTimeoutMs = 6000,
      connectionTimeoutMs = 10000, maxInFlightRequests = Int.MaxValue, time = Time.SYSTEM)

  @throws[FileNotFoundException]
  def deleteFile(path: File): Boolean = {
    if (!path.exists) throw new FileNotFoundException(path.getAbsolutePath)
    var ret: Boolean = true
    if (path.isDirectory) for (f <- path.listFiles) {
      ret = ret && deleteFile(f)
    }
    ret && path.delete
  }

  def randomString(length: Int = 10, numbers: Boolean = false): String = {
    val str = scala.util.Random.alphanumeric.take(length).mkString
    if (!numbers) str.replaceAll("[0-9]", "") else str
  }

  def swallow(action: => Unit) {
    try {
      action
    } catch {
      case e: Throwable => logger.warn(e.getMessage, e)
    }
  }

  class MockableKsqlRestClient(client: Client) extends KsqlRestClient("http://localhost:8080")

  def mockQueryStream(mockResponse: Response): KsqlRestClient.QueryStream = {
    classOf[KsqlRestClient.QueryStream].getDeclaredConstructors
      .filter(_.getParameterCount == 1)
      .map(c => {
        c.setAccessible(true)
        c
      }).head.newInstance(mockResponse).asInstanceOf[KsqlRestClient.QueryStream]
  }

  def implementedMethods[T <: AnyRef](implicit ct: ClassTag[T]): Seq[String] = {
    ct.runtimeClass.getMethods.filter(_.getDeclaringClass == ct.runtimeClass).map(_.getName)
  }

  def reflectMethods[T <: AnyRef](methods: Seq[String], implemented: Boolean, obj: T)
                                 (implicit tt: TypeTag[T], ct: ClassTag[T]): Seq[() => Any] = {

    val ksqlPackage = "com.github.mmolimar.ksql"
    val declarations = for {
      baseClass <- typeTag.tpe.baseClasses
      if baseClass.fullName.startsWith(ksqlPackage)
    } yield baseClass.typeSignature.decls

    declarations.flatten
      .filter(_.overrides.nonEmpty)
      .filter(ms => methods.contains(ms.name.toString) == implemented)
      .map(_.asMethod)
      .filter(!_.isProtected)
      .map(m => {

        val args = new Array[AnyRef](if (m.paramLists.isEmpty) 0 else m.paramLists.head.size)
        if (m.paramLists.nonEmpty)
          for ((paramType, index) <- m.paramLists.head.zipWithIndex) {
            args(index) = paramType.info.typeSymbol match {
              case tof if tof == typeOf[Byte].typeSymbol => Byte.box(0)
              case tof if tof == typeOf[Boolean].typeSymbol => Boolean.box(false)
              case tof if tof == typeOf[Short].typeSymbol => Short.box(0)
              case tof if tof == typeOf[Int].typeSymbol => Int.box(0)
              case tof if tof == typeOf[Double].typeSymbol => Double.box(0)
              case tof if tof == typeOf[Long].typeSymbol => Long.box(0)
              case tof if tof == typeOf[Float].typeSymbol => Float.box(0)
              case tof if tof == typeOf[String].typeSymbol => ""
              case e => null
            }
          }

        val mirror = runtimeMirror(classTag[T].runtimeClass.getClassLoader).reflect(obj)
        val method = mirror.reflectMethod(m)
        () =>
          try {
            method(args: _*)
          } catch {
            case t: InvocationTargetException => throw t.getCause
          }

      })
  }

}
