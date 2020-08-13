package com.tlh.api.demo.udf

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

/**
 * @Comment 表函数
 * @Author: tlh
 * @Date 2020/8/12 8:58
 * @Version 1.0
 */
object TableFunction {

  def main(args: Array[String]): Unit = {
    //0.环境初始化
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //生产并行度,需要根据环境决定(资源,计算精确度)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //1.文件数据源
    val inputPath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputstream: DataStream[String]= env.readTextFile(inputPath)

    //2.DS转换
    val dataStream = inputstream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    //3.注册表 table api
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
    val split = new Split("_")

    val resultTable = sensorTable
      .joinLateral(split('id) as ('word,'length))
      .select('id, 'ts,'word,'length)
    resultTable.toAppendStream[Row].print("result")

    //3.1 sql
    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("split",split)

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |id, ts, word, length
        |from
        | sensor,lateral table (split(id)) as splitid(word,length)
      """.stripMargin)
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("table funtion test")

  }
}

class Split(separator: String) extends TableFunction[(String, Int)]{
  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}

