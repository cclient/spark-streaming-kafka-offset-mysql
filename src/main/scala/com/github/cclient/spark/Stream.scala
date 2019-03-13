package com.github.cclient.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object Stream  extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(Stream.getClass)

  def createStreamingContextContext(env:String,topic:String,group:String) = {
    val master = "local"
    val topics = Array(topic)
    val conf: Config = Config(env)
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName("consume_"+topic)
      .set("spark.io.compression.codec","snappy")
    val offsetTable=conf.GetKfkOffsetTable()
    val offsetMysqlConf = conf.GetMysqlConf(offsetTable)
    logger.info("offsetMysqlConf",offsetMysqlConf)
    val kafkaParams=conf.GetKFKConf(group)
    logger.info("kafkaParams",kafkaParams)
    val streamingContext = new StreamingContext(sparkConf, Seconds(60))
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    //get latest offset
    val step_offset = Offset.getLastestStepAndPartitions(sparkSession, offsetMysqlConf,offsetTable, topic,group)
    val step = step_offset._1
    val topicPartition=step_offset._2
    logger.info("topicPartition",topicPartition)
    val kfkStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferBrokers, Subscribe[String, String](topics, kafkaParams, topicPartition))
    val valueStream = kfkStream.map(_.value())
    var broadcastStep = streamingContext.sparkContext.broadcast(step)
    //do some work
    val count=valueStream.count()
    count.print()
    //set new offset
    val newStep = Offset.saveOffset(kfkStream,offsetMysqlConf,offsetTable, broadcastStep.value, group)
    broadcastStep.unpersist()
    broadcastStep = streamingContext.sparkContext.broadcast(newStep + 1)
    streamingContext
  }

  var errorCount = 0
  val retryMax = 5
  val retryInterval = 10 //minute

  def loopSscStart(ssc: StreamingContext)= {
    //todo 现在的异常恢复方案只适用于偶发异常，如es集群暂时失败，如果是必现的异常，如数据错误导致写es失败，会无限重复异常再恢复，日志会把硬盘写满，暂设恢复5000次，后期考虑报警。
    var lastException: Option[Exception] = None
    while (errorCount < retryMax) {
      logger.warn("restart")
      ssc.start()
      try {
        ssc.awaitTermination()
      } catch {
        //        If exception is caused by network jitter don't accumulation errorCount
        //        case e: EsHadoopNoNodesLeftException =>
        //          logger.error("EsHadoopNoNodesLeftException", e)
        case e: Exception =>
          lastException = Option(e)
          logger.error("Other Exception", e)
          errorCount = errorCount + 1;
      } finally {
        TimeUnit.MINUTES.sleep(retryInterval)
      }
    }
    //    alert
    logger.error("last Exception", lastException.get)
  }

  def main(args: Array[String]) {
    val    env="prod"
    val    topic="task-response"
    val     group="extract"
//    val argError = "Usage: Stream <prod/test> <topic> <group>"
//    if (args.length < 3) {
//      System.err.println(argError)
//      System.exit(1)
//    }
    //config
//    val Array(env, topic, group) = args
    val streamingContext=createStreamingContextContext(env, topic, group)
    loopSscStart(streamingContext)
  }
}
