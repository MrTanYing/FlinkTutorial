package com.tlh.api.demo.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/4 13:50
 * @Version 1.0
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //1.获取外部环境参数
        val parameterTool : ParameterTool = ParameterTool.fromArgs(args)
        val host: String = parameterTool.get("host")
        val port: Int = parameterTool.getInt("port")

        //2.创建流处理环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //默认并行度 电脑2x4
//        env.setParallelism(8)

        val textDStream: DataStream[String]= env.socketTextStream(host, port)
        //3.flatMap和Map需要引用的隐式转换
        import org.apache.flink.api.scala._
        val dataStream: DataStream[(String,Int)]= textDStream.
          flatMap(_.split(" "))
          .filter(_.nonEmpty).
          map((_, 1))
          .keyBy(0).
          sum(1)
        dataStream.print().setParallelism(1)

        //4.启动executor,执行任务
        env.execute("Socket stream word count")
    }

}
