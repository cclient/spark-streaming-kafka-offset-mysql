package com.github.cclient.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}



object Offset  extends Serializable {
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getNowMysqlDateFormatStr(): String = {
    val calendar = Calendar.getInstance()
    calendar.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    df.format(calendar.getTime)
  }

  val logger: Logger = LoggerFactory.getLogger(Offset.getClass)

  def getLastestStepAndPartitions(sparkSession: SparkSession, mysqlConf: Map[String, String],offsetTable:String, topic: String, group: String) = {
    sparkSession.sqlContext.read.format("jdbc").options(mysqlConf).option("dbtable", offsetTable).load()
      .createOrReplaceTempView(offsetTable)
    //    todo 失败，暂用两次查询实现
    //    spark.sql("select * from kfk_offset  where step = (select max(step) from kfk_offset)")
    val lastStep = sparkSession
      .sql(s"select max(step) as step from `$offsetTable` where topic='$topic' and `group`='$group'")
      .collect().head.getAs[Int](0)
    val offset = sparkSession
      .sql(s"select * from `$offsetTable`  where topic='$topic' and `group`='$group' ")
      .filter("step=" + lastStep).collect()
      .map(row => {
        (row.getAs[Int]("partition"), row.getAs[Long]("until"))
      })
      .foldLeft[Map[TopicPartition, Long]](Map[TopicPartition, Long]())((map, row) => {
      map.updated(new TopicPartition(topic, row._1), row._2)
    })
    (lastStep, offset)
  }

  implicit def sparkKFKOffsetMysqlStoreFunctions[K,V](rdd: RDD[ConsumerRecord[K, V]]): SparkKFKOffsetMysqlStoreFunctions[K,V] = new SparkKFKOffsetMysqlStoreFunctions(rdd)

  def saveOffset[K,V](kfkStream: InputDStream[ConsumerRecord[K, V]], offsetMysqlConf: Map[String, String], offsetTable:String,broadcastStep: Int, group: String) = {
    var tmpBroadcastStep = broadcastStep
    kfkStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val rddOffset=rdd.mapPartitions(_ => {
        val offset: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        List[OffsetRange](offset).toIterator
      })
        .filter(offset => offset.count()>0)
      //      calculate by kafka offset don't cause run job
      val consumeCount=offsetRanges.map(_.count()).sum
      if(consumeCount>0){
        tmpBroadcastStep = tmpBroadcastStep + 1
        val rddStoreOffset=rddOffset.map(o => StoreOffset(o.topic, group, tmpBroadcastStep, o.partition, o.fromOffset, o.untilOffset, o.count(),getNowMysqlDateFormatStr))
        var lastException: Option[Exception] = None
        logger.info(s"current offset step: $tmpBroadcastStep, consumeCount: $consumeCount")
        try {
          rdd.storeOffset(offsetMysqlConf,offsetTable,rddStoreOffset)
        } catch {
          case e: Exception =>
            lastException = Some(e)
            logger.error("store mysql offset Exception", e)
        }
        if(lastException.isEmpty){
          kfkStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }else{
          throw lastException.get
        }
      }
    }
    tmpBroadcastStep
  }

  class SparkKFKOffsetMysqlStoreFunctions[K,V](rdd: RDD[ConsumerRecord[K, V]]) extends scala.AnyRef with scala.Serializable {
    def storeOffset(offsetMysqlConf: Map[String, String],offsetTable:String, storeOffset:RDD[StoreOffset])= {
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      storeOffset.cache()
      storeOffset.foreach(offset=>logger.info(s"offset: $offset"))
      storeOffset.toDF("topic", "group", "step", "partition", "from", "until", "count", "datetime")
        .write
        .format("jdbc")
        .mode("append")
        .options(offsetMysqlConf)
        .option("dbtable", offsetTable)
        .save()
    }
  }

}
