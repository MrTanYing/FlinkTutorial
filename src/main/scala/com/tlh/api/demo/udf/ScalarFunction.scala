package com.tlh.api.demo.udf

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @Comment 标量函数
 * @Author: tlh
 * @Date 2020/8/12 8:58
 * @Version 1.0
 */
object ScalarFunction {

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
    val hashCode = new HashCode(2)
    val resultTable = sensorTable.select('id, 'ts, hashCode('id))
    resultTable.toAppendStream[Row].print("result")
    println("----------------------------")

    //3.1 sql
    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("hashcode",hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id,ts,hashcode(id) from sensor")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar funtion test")

  }

}
class HashCode(factor: Int) extends ScalarFunction{
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}
