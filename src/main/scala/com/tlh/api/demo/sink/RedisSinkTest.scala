package com.tlh.api.demo.sink

import com.tlh.api.demo.source.MySensorSource
import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/7 22:44
 * @Version 1.0
 */
object RedisSinkTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //读取数据
        val dataStream = env.addSource(new MySensorSource)

        dataStream.print()
        val conf = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop103")
            .setPort(6379)
            .build()
        dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))
        env.execute("redis sink test")
    }
}

class MyRedisMapper extends RedisMapper[SensorReading]{
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
    }

    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
