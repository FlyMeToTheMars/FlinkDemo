package com.hugh.sink

import java.util

import com.hugh.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.table.descriptors.Elasticsearch
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * @program: PracticeByHugh
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-24 00:34
 **/
object flinkConnect2Es {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = value.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )

    val httpHosts = new util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("192.168.229.133",9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          print("saving data:" + t)
          // 写入es必须map或者json
          val m = new util.HashMap[String, String]()
          m.put("sensor_id", t.id)
          m.put("temperature", t.temperature.toString)
          m.put("ts", t.temperature.toString)

          // 创建index request 准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(m)

          // 利用index发送请求，写入数据
          requestIndexer.add(indexRequest)

          println("data saved.")
        }
      }
    )

    sensorStream.addSink( esSinkBuilder.build())

    env.execute("es sink")

  }

}
