package com.tlh.api.demo.source

import java.util.Properties

import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * @Comment 数据源调试
 * @Author: tlh
 * @Date 2020/8/6 9:43
 * @Version 1.0
 */
object SourceTest {

    def main(args: Array[String]): Unit = {
        //上下文环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //1.从集合中读取数据
        val stream1 = env
          .fromCollection(List(
              SensorReading("sensor_1", 1547718199, 35.8),
              SensorReading("sensor_6", 1547718201, 15.4),
              SensorReading("sensor_7", 1547718202, 6.7),
              SensorReading("sensor_10", 1547718205, 38.1)
          ))
//        stream1.print("stream1").setParallelism(1)

        //2.从文件中读取数据
        val inputPath = "D:\\IdeaProjects\\BigDataTlh\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
        val stream2: DataStream[String]= env.readTextFile(inputPath)
//        stream2.print("stream2").setParallelism(1)

        //3.从kafka中读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers","hadoop102:9092")
        properties.setProperty("group.id","consumer-group")
        val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//        stream3.print()

        // 4. 自定义Source
        val stream4 = env.addSource( new MySensorSource() )

        stream4.print()
        env.execute("source test")

    }

    case class SensorReading(id: String,timestamp: Long, temperature: Double)

}

// 自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
    // 定义一个标识位flag，用来表示数据源是否正常运行发出数据
    var running: Boolean = true

    override def cancel(): Unit = running = false

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
        // 定义一个随机数发生器
        val rand = new Random()

        // 随机生成一组（10个）传感器的初始温度: （id，temp）
        var curTemp = 1.to(10).map( i => ("sensor_" + i, rand.nextDouble() * 100) )

        // 定义无限循环，不停地产生数据，除非被cancel
        while(running){
            // 在上次数据基础上微调，更新温度值
            curTemp = curTemp.map(
                data => (data._1, data._2 + rand.nextGaussian())
            )
            // 获取当前时间戳，加入到数据中，调用ctx.collect发出数据
            val curTime = System.currentTimeMillis()
            curTemp.foreach(
                data => ctx.collect(SensorReading(data._1, curTime, data._2))
            )
            // 间隔500ms
            Thread.sleep(500)
        }
    }
}
