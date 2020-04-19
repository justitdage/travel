package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants}
import com.travel.utils.{HbaseTools, JsonParse}
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingMaxwellKafka {
  def main(args: Array[String]): Unit = {


    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(Constants.VECHE)
    val conf = new SparkConf().setMaster("local[4]").setAppName("sparkMaxwell")
    val group_id: String = "vech_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "earliest", // earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    // val streamingContext = new StreamingContext(conf,Seconds(5))
    //获取streamingContext
    val ssc: StreamingContext = new StreamingContext(context, Seconds(1))


    /**
     * streamingContext: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], group: String,matchPattern:String
     */
    val getDataFromKafka: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(ssc, kafkaParams, topics, group_id, "veche")

    //将我么每个不同表的数据，接入到hbase四张表里面去
    getDataFromKafka.foreachRDD(eachRdd => {
      eachRdd.foreachPartition(eachPartition => {
        val conn: Connection = HbaseTools.getHbaseConn
        eachPartition.foreach(eachLine => {
          val jsonStr: String = eachLine.value() //获取我们一整条数据 数据格式，是json格式的
          val parse: (String, Any) = JsonParse.parse(jsonStr)
          HbaseTools.saveBusinessDatas(parse._1, parse, conn)
        })
        HbaseTools.closeConn(conn)
      })
      //每个分区的数据处理完成，更新offset的值
      val ranges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //  getDataFromKafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      for (eachRange <- ranges) {
        val startOffset: Long = eachRange.fromOffset
        val endOffset: Long = eachRange.untilOffset
        val topic: String = eachRange.topic
        val partition: Int = eachRange.partition
        HbaseTools.saveBatchOffset(group_id, topic, partition + "", endOffset)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  }
