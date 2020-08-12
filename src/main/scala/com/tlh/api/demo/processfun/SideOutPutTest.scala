package com.tlh.api.demo.processfun

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingAlignedProcessingTimeWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/9 18:57
 * @Version 1.0
 */
object SideOutPutTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val inputStream = env.socketTextStream("hadoop102", 7777)
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
        val resultStream = dataStream
          .map(data => (data.id, data.temperature, data.timestamp))
          .keyBy(_._1)
          //          .window(TumblingEventTimeWindows.of(Time.seconds(15)) //滚动时间窗口
          //          .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3)) //滑动时间窗口
          //          .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  //会话窗口
          .countWindow(15)
          .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
        resultStream.print()
        env.execute("window test")

    }

}

