package com.travel.programApp

import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants, JedisUtil}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.{Cell, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * 将offset的值维护在hBase
 */
object StreamingKafka {

  def main(args: Array[String]): Unit = {

        val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
        val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
        val group:String = "gps_consum_group"
        val kafkaParams = Map[String,Object](
              "bootstrap.servers" -> brokers,
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> group,
          "auto.offset.reset" -> "latest", // earliest,latest,和none
          "enable.auto.commit" ->(false: java.lang.Boolean)
    )


    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafka")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")

    val streamingContext: StreamingContext = new StreamingContext(context, Seconds(1))

//
//    val conn: Connection = HbaseTools.getHbaseConn
//    val admin: Admin = conn.getAdmin
//    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
//      val hbaseoffsetstoretable: HTableDescriptor = new HTableDescriptor(Constants.HBASE_OFFSET_STORE_TABLE)
//      hbaseoffsetstoretable.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
//      admin.createTable(hbaseoffsetstoretable)
//      admin.close()
//    }
//
//    val partitionToLong: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()
//
//
//    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
//    //rowkey group:topic
//    for ( eachTopic <- topics) {
//      val get: Get = new Get((group + ":" + eachTopic).getBytes)
//      val result: Result = table.get(get)
//      val cells: Array[Cell] = result.rawCells()
//      for (cell <- cells){
//        //offset值
//        val offset: String = Bytes.toString(cell.getValue)
//
//        //列明
//        val qualifier: String = Bytes.toString(cell.getQualifier)
//
//        val strings: Array[String] = qualifier.split(":")
//
//        val partition: TopicPartition = new TopicPartition(strings(1), strings(2).toInt)
//
//        //获取到我们的offset的值
//        partitionToLong.+=(partition -> offset.toLong)
//      }
//    }
//
//
//    /**
//     * pattern: ju.regex.Pattern,
//     * kafkaParams: collection.Map[String, Object]
//     *
//     *
//     * pattern: ju.regex.Pattern,
//     * kafkaParams: collection.Map[String, Object],
//     * offsets: collection.Map[TopicPartition, Long]
//     */
//
//    //如何设置hBase来存取offset
//    val result = if(partitionToLong.size>0){
//
//      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams,partitionToLong);
//
//    }else{
//      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams);
//
//    }
//
//
//    /**
//     * ssc: StreamingContext,
//     * locationStrategy: LocationStrategy,
//     * consumerStrategy: ConsumerStrategy[K, V]
//     */
//    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent, result)
//
//    val resultDStream: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, ("(.*)gps_topic"))


    val result: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, ("(.*)gps_topic"))

    result.foreachRDD( eachRdd =>{

        if(!eachRdd.isEmpty()) {
          //若果使用maoPartition会产生shuffle
          eachRdd.foreachPartition(eachPartition => {

            val conn: Connection = HbaseTools.getHbaseConn
            val jedis: Jedis = JedisUtil.getJedis

            eachPartition.foreach(record => {
              //获取每一条逻辑
              //connection:Connection,jedis:Jedis, eachLine: ConsumerRecord[String, String]
              val consumerRecord: ConsumerRecord[String, String] = HbaseTools.saveToHBaseAndRedis(conn, jedis, record)
            })
            JedisUtil.returnJedis(jedis)
            conn.close()
          })


          val ranges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //result.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
          for (eachRange <- ranges){
            val startOffset: Long = eachRange.fromOffset
            val endOffset: Long = eachRange.untilOffset
            val topic: String = eachRange.topic
            val partition: Int = eachRange.partition
            HbaseTools.saveBatchOffset(group,topic,partition+"",endOffset)
          }
        }
    })



    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
