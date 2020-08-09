package com.tlh.api.demo.transform

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Comment 转换算子
 * @Author: tlh
 * @Date 2020/8/6 10:55
 * @Version 1.0
 */
object TransformTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //0.读取数据
        val inputPath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
        val inputStream = env.readTextFile(inputPath)
        //1.map
        //sum()
        //min()
        //max()
        //minBy()
        //maxBy()
        val dataStream = inputStream
          .map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
        val aggStream = dataStream
          .keyBy("id")
          .sum("temperature")
//        aggStream.print()

        //reduce 输出最小温度值,最近时间戳,reduce

        val reduceStream = dataStream
          .keyBy("id")
          .reduce(new MyReduceFunction)
//        reduceStream.print()

        //4.1多流转换
        val splitStream = dataStream.split(data => {
            if (data.temperature > 30.0) Seq("high") else Seq("low")
        })
        val highTempStream = splitStream.select("high")
        val lowTempStream = splitStream.select("low")
        val allTempStream = splitStream.select("high","low")
//        highTempStream.print()

        val warningStream = highTempStream.map(data => (data.id, data.temperature))

        //4.2 合流 connect
        val connectStreamn = warningStream.connect(lowTempStream)

        //4.3 coMap 分流处理
        val coMapResultStream = connectStreamn.map(
            warningData => (warningData._1, warningData._2, "warning"),
            lowTempData => (lowTempData.id, "healthy")
        )

        //4.3 union
        val unionStream = highTempStream.union(lowTempStream, allTempStream)
        coMapResultStream.print("coMap")

        env.execute("transform test")
    }



}

class MyReduceFunction extends ReduceFunction[SensorReading]{
    override def reduce(s1: SensorReading, s2: SensorReading): SensorReading = {
        SensorReading(s1.id,s2.timestamp,s1.temperature.min(s2.temperature))
    }
}
