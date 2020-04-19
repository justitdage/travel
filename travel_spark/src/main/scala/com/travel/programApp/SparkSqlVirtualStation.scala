package com.travel.programApp

import java.util

import com.travel.common.{Constants, District}
import com.travel.utils.{HbaseTools, SparkUtils}
import com.uber.h3core.H3Core
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.{GeometryFactory, Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSqlVirtualStation {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkSqlVirtualStation")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val context: SparkContext = session.sparkContext

    context.setLogLevel("WARN")

    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum","node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort","2181")
    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(session, hconf)

    //注册成为1张表
    hbaseFrame.createOrReplaceTempView("order_df")

//    val h3: H3Core = H3Core.newInstance()   //序列化错误 在driver段生成
//
//    //自定义UDF 函数 将经纬度 抓换成h3编码
//
//    session.udf.register("localH3",new UDF3[String,String,Int,Long]{
//      override def call(lat: String, lng: String, result: Int):Long = {
//        h3.geoToH3(lat.toDouble,lng.toDouble,result)
//      }
//    },DataTypes.LongType)
//
//    val order_sql =
//      """
//        |select order_id,city_id,starting_lng,starting_lat,localH3(starting_lat,starting_lng,12) as h3Code
//        |from order_df
//        |""".stripMargin
//
//    val gridDF: DataFrame = session.sql(order_sql)
//
//    gridDF.createOrReplaceTempView("order_grid")
//
//    val sql: String =
//      s"""
//         | select
//         |order_id,
//         |city_id,
//         |starting_lng,
//         |starting_lat,
//         |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
//         | from order_grid  join (
//         | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
//         | ) groupcount on order_grid.h3code = groupcount.h3code
//         |having(rn=1)
//      """.stripMargin
//
//      session.sql(sql) //计算出来有多少个虚拟车站

    //虚拟车展的计算
    val virturl_rdd: RDD[Row] = SparkUtils.getVirtualFrame(session)

    //每个虚拟车站属于哪个区


    //获取海口市没余个区的边界
    val broadCast: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(session)

    val finalSaveRow: RDD[mutable.Buffer[Row]] = virturl_rdd.mapPartitions(eachPartition => {
      val factory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      val reader: WKTReader = new WKTReader(factory)

      //得到每个区的边界
      val wktpoloygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(broadCast, reader)
      eachPartition.map(row => {
        val lng: String = row.getAs[String]("starting_lng")
        val lat: String = row.getAs[String]("starting_lat")
        val wktPoint = "POINT(" + lng + " " + lat + ")"

        //判断虚拟车站属于哪个边界
        val point: Point = reader.read(wktPoint).asInstanceOf[Point]

        val rows: mutable.Buffer[Row] = wktpoloygons.map(pology => {
          if (pology._2.contains(point)) {
            val fields: Array[Any] = row.toSeq.toArray ++ Seq(pology._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }

        }).filter(null != _)
        rows

      })

    })

    val rowRdd: RDD[Row] = finalSaveRow.flatMap(x => x)
    HbaseTools.saveOrWriteData(hconf,rowRdd,Constants.VIRTUAL_STATION)



  }

}
