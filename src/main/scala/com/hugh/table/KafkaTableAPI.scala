package com.hugh.table

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-05-17 13:25
 **/
class KafkaTableAPI {

}

/**
 * Table转换成ds
 * 两种模式
 * Append模式和Retract模式
 *
 * 可以查看执行计划
 * tableEnv.explain(resultTable)
 *
 * */

/**
 * flink对SQL的查询：动态表
 * 持续查询：会生成另一张动态表
 * 连续查询永远不会终止，并会生成另一个动态表
 * 查询会不断更新动态结果表
 * */