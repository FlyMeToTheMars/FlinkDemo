package com.hugh.table

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @program: FlinkDemo
 * @description: 针对输出 读取
 * @author: Fly.Hugh
 * @create: 2020-05-17 03:09
 **/
object TableAPIOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 将ds转换成table,这里的字段可以自行更改顺序
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id as 'myid, 'temperature, 'timestamp)
    sensorTable.printSchema()
    sensorTable.toAppendStream[(String,Double,Long)].print()

    // ==================> output
    val filePath = "C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReadingout.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) // 定义读取数据之后的格式化方法
      .withSchema(new Schema()
        .field("myid",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
        .field("timestamp",DataTypes.BIGINT())
      ).createTemporaryTable("outputTable")

    sensorTable
        .insertInto("outputTable")

    env.execute("output")

  }
}
/*
* 更新模式 Update Mode
* Append Mode 在追加模式下，表（动态表）和外部连接器只交换插入（Insert）消息。
* Retract Mode  在撤回模式下，表和外部连接器交换的是：添加（Add）和撤回（Retract）消息。
* Upsert Mode 这种模式和Retract模式的主要区别在于，Update操作是用单个消息编码的，所以效率会更高。
*
* */