package com.tlh.api.demo.tableapi

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/10 23:25
 * @Version 1.0
 */
object TableApiDemo {

  def main(args: Array[String]): Unit = {
    //0.上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.数据源
    val inputPath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String]= env.readTextFile(inputPath)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    //3.创建table执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //4.基于流创建一张表
    val dataTable = tableEnv.fromDataStream(dataStream)
    //5.表的 table api 操作

    /*val resultTable = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")
    resultTable.toAppendStream[(String,Double)].print("result")
   */
    //6.sql 操作
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sql = "select id,temperature from dataTable where id ='sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String,Double)].print("result sql")
    env.execute("table api demo")
  }


}
