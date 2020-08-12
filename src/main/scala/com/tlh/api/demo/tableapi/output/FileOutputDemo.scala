package com.tlh.api.demo.tableapi.output

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/10 23:25
 * @Version 1.0
 */
object FileOutputDemo {

  def main(args: Array[String]): Unit = {
    //0.上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1.创建table执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //2.连接外部系统,读取数据,注册表

    //2.1 文件数据源
    val filePath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
        .withFormat(new Csv())
        .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("temperature",DataTypes.DOUBLE()))
          .createTemporaryTable("fileinputTable")

    //2.2 kafka数据源

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "hadoop102:2181")
      .property("bootstrap.servers", "hadoop102:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

//    val kafkaInputTable: Table = tableEnv.from("kafkaInputTable")
//    kafkaInputTable.toAppendStream[(String, Long, Double)].print()


    //3.1 table api
    val sensorTable = tableEnv.from("fileinputTable")
    val resultTable = sensorTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")
//    resultTable.toAppendStream[(String,Double)].print("result")

    //3.2 sql
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from kafkaInputTable
        |where id = 'sensor_1'
      """.stripMargin)
    resultSqlTable.toAppendStream[(String, Double)].print("sql")

    env.execute("table api demo")
  }


}
