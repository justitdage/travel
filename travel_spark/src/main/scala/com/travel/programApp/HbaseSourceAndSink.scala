package com.travel.programApp

import java.util

import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

object HbaseSourceAndSink {

  /**
   * 自定义sparkSql额数据源 实现读取一张表的数据 然后插入到另一张表
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("HbaseSourceAndSink")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val context: SparkContext = session.sparkContext

    //通过spark读取hbase数据 得到一张表 name 列 score 列  映射成为另一张表
    //schema的定义 2个schema  hbase的schema 自定义表的schema
    //默认自带一个数据源  协商自定义数据源的类路径
    val data: DataFrame = session.read.format("com.travel.programApp.HBaseSoure")
      .option("hbase.table.name", "spark_hbase_sql")
      .option("schema", "`name` STRING,`score` STRING")
      .option("cf.cc", "cf:name,cf:score")
      //将sparkContext 序列化成字符串
      //.option("context",context)  //不能往下传
      .load()


    data.explain()
    data.createOrReplaceTempView("sparkHbaseSql")
    data.printSchema()

    val result: DataFrame = session.sql("select * from sparkHbaseSql")

    result.show()


    //写入
//    data.write.format("com.travel.programApp.HBaseSoure")  实现WriteSupport接口




  }

}

class HBaseSoure extends DataSourceV2 with ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase.table.name").get()
    val schema: String = options.get("schema").get()
    val cfCc: String = options.get("cf.cc").get()
      new HBaseDataSourceReader(tableName:String,schema:String,cfCc:String)
  }
}


class HBaseDataSourceReader(tableName:String,schema:String,cfCc:String) extends DataSourceReader{
  //获取我们定义的表的schema
  private val structType: StructType = StructType.fromDDL(schema)
  override def readSchema(): StructType = {
    structType
  }

  /**
   * 读取输的工厂类
   * @return
   */
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import  scala.collection.JavaConverters._
    //序列
    Seq{
      new HbaseDataReaderFactory(tableName,schema,cfCc).asInstanceOf[DataReaderFactory[Row]]
    }.asJava   //scala集合转成java
  }

}

class HbaseDataReaderFactory(tableName:String,schema:String,cfCc:String) extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = {
    new HbaseDataReader(tableName,schema,cfCc)
  }
}

/**
 *
 * @param tableName
 * @param schema
 * @param cfCc
 */
class HbaseDataReader(tableName:String,schema:String,cfCc:String) extends DataReader[Row]{

  private var hbaseConnect:Connection = null
  private var resultScanner:ResultScanner = null
  private var nextResult:Result = null

  def getIterator: Iterator[Seq[AnyRef]] = {
      hbaseConnect = HbaseTools.getHbaseConn
    val table: Table = hbaseConnect.getTable(TableName.valueOf(tableName))
    resultScanner = table.getScanner(new Scan())


    import scala.collection.JavaConverters._
    val iterator: Iterator[Seq[String]] = resultScanner.iterator().asScala.map(eachRecord => {
      val name: String = Bytes.toString(eachRecord.getValue("cf".getBytes, "name".getBytes))
      val score: String = Bytes.toString(eachRecord.getValue("cf".getBytes, "score".getBytes))
      Seq(name, score)
    })
    iterator

  }

  val data:Iterator[Seq[AnyRef]] = getIterator


  /** iterator.hasNext()
   * 循环获取下一条
   * @return
   */
  override def next(): Boolean = {
    data.hasNext
  }

  /**
   * 获取一条数据
   * @return
   */
  override def get(): Row = {
    val seq: Seq[AnyRef] = data.next()
    Row.fromSeq(seq)
  }

  /**
   * 关闭资源
   */
  override def close(): Unit = {
    hbaseConnect.close()
  }
}

