package com.github.cclient.spark

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer

class Config(conf: Map[String, String]) {
  def GetMysqlConf(table: String): Map[String, String] = {
    Map[String, String](
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> s"jdbc:mysql://${conf(Config.MYSQL_HOST)}:${conf(Config.MYSQL_PORT)}/${conf(Config.MYSQL_DB)}",
      "dbtable" -> table,
      "user" -> conf(Config.MYSQL_USER),
      "password" -> conf(Config.MYSQL_PASSWORD),
      "fetchsize" -> "3"
    )
  }

  def GetKFKConf(group: String): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> conf(Config.KAFKA_BOOTSTRAP_SERVER),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",//latest
      "max.poll.records" -> "1024",
      "enable.auto.commit" -> "false"
    )
  }

  def GetKfkOffsetTable(): String = {
    conf(Config.MYSQL_KFK_OFFSET)
  }
}

object Config {
  val MYSQL_HOST = "mysql.host"
  val MYSQL_PORT = "mysql.port"
  val MYSQL_USER = "mysql.user"
  val MYSQL_PASSWORD = "mysql.passwd"
  val MYSQL_DB = "mysql.db"
  val MYSQL_KFK_OFFSET = "mysql.kfk_offset"
  val KAFKA_BOOTSTRAP_SERVER = "kafka.bootstrap-server"

  def apply(env: String) = {
    val properties = new Properties()
    val mysqlIuputStream: InputStream = this.getClass.getResourceAsStream(s"/config-mysql-$env.properties")
    val kfkIuputStream: InputStream = this.getClass.getResourceAsStream(s"/config-kfk-$env.properties")
    properties.load(mysqlIuputStream)
    var confMap=Map(
      MYSQL_HOST -> properties.getProperty(MYSQL_HOST),
      MYSQL_PORT -> properties.getProperty(MYSQL_PORT),
      MYSQL_USER -> properties.getProperty(MYSQL_USER),
      MYSQL_PASSWORD -> properties.getProperty(MYSQL_PASSWORD),
      MYSQL_DB -> properties.getProperty(MYSQL_DB),
      MYSQL_KFK_OFFSET -> properties.getProperty(MYSQL_KFK_OFFSET)
    )
    properties.load(kfkIuputStream)
    confMap=confMap.+( KAFKA_BOOTSTRAP_SERVER -> properties.getProperty(KAFKA_BOOTSTRAP_SERVER))
    new Config(confMap)
  }
}
