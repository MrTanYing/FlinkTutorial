package com.tlh.api.demo.tableapi.output

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/10 23:25
 * @Version 1.0
 */
object EsOutputDemo {

  def main(args: Array[String]): Unit = {
    //0.上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1.创建table执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //2.连接外部系统,读取数据,注册表

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

    //3.1 table api
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")


    tableEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sinkTest")
        .property("zookeeper.connect", "hadoop02:2181")
        .property("bootstrap.servers", "hadoop02:9092"))
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")

    env.execute("kafka pipeline demo")
  }

}
