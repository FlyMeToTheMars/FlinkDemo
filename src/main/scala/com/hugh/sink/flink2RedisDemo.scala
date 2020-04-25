package com.hugh.sink

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-23 00:00
 **/
object flink2RedisDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")


    val sensorStream: DataStream[SensorReading] = value.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )

    val redisConf = new FlinkJedisPoolConfig.Builder()
        .setHost("192.168.229.132")
        .setPort(6379)
        .setPassword("hadoop001")
        .build()

    sensorStream.addSink(new RedisSink[SensorReading](redisConf,new MyRedisMapper))

    env.execute("flink2Redis")

  }

  class MyRedisMapper extends RedisMapper[SensorReading]{

    // The type of the element handled by this
    override def getCommandDescription: RedisCommandDescription = {
      // HSET key field value. Assign key(table name) here
      new RedisCommandDescription( RedisCommand.HSET,"SensorReading")
    }

    override def getKeyFromData(data: SensorReading): String = {
      data.id
    }

    override def getValueFromData(data: SensorReading): String = {
      data.temperature.toString
    }
  }
}
