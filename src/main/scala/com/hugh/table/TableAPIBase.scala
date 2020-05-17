package com.hugh.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.table.sinks.CsvTableSink.CsvFormatter

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-05-16 23:43
 **/
object TableAPIBase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 从外部系统读取数据 ， 再用环境注册表
    val filePath = "C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) // 定义读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
      .field("timestamp",DataTypes.BIGINT())
      .field("temperature",DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    val sensorTable: Table = tableEnv.from("inputTable")
    sensorTable.toAppendStream[(String,Long,Double)].print()

    // 过滤投影
    sensorTable.select('id,'temperature)
    // SQL简单查询
    tableEnv.sqlQuery(
      """
        |select id,temperature from inputTable where id = 'sensor_1'
        |""".stripMargin)

    /*
    * 聚合函数的输出流数据比较特殊
    * 这条数据比较奇特，前面会输出一个boolean 代表这条数据是否是过期的数据
    * 如果是相同key过来的话，那么每次会输出两条数据，第一条数据是false 第二条数据会输出一个true
    *
    * */

    // kafka
    tableEnv.connect(new Kafka()
      .version("2.3")
        .topic("sensor")
        .property("bootsrap.servers","localhost:9092")
        .property("zookeeper.connect","localhost:2181")

    )
      .withFormat( new Csv())
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE()))
        .createTemporaryTable("kafkaInputTable")




    env.execute("table")
  }
}
