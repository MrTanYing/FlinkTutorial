package com.tlh.api.demo.sink

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/7 22:44
 * @Version 1.0
 */
object FileSinkTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //读取文件数据
        val inputPath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
        val inputStream = env.readTextFile(inputPath)
        //数据转换
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        dataStream.print()
        dataStream.addSink(
            StreamingFileSink.forRowFormat(
                new Path("D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\out.txt"),
                new SimpleStringEncoder[SensorReading]()
            ).build()
        )
        env.execute("file sink test")
    }

}
