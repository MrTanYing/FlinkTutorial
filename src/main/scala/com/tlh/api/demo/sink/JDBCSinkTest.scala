package com.tlh.api.demo.sink

import java.sql.{DriverManager, PreparedStatement}

import com.mysql.jdbc.Connection
import com.tlh.api.demo.source.MySensorSource
import com.tlh.api.demo.source.SourceTest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/7 22:44
 * @Version 1.0
 */
object JDBCSinkTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //读取数据
        val dataStream = env.addSource(new MySensorSource)

        dataStream.print()
        dataStream.addSink(new MyJdbcSinkFunc)
        env.execute("jdbc sink test")
    }

}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading]{
    var conn: Connection = _
    var insretStmt: PreparedStatement = _

    var updateStmt: PreparedStatement = _


    override def invoke(value: SensorReading): Unit = {
        updateStmt.setDouble(1,value.temperature)
        updateStmt.setString(2,value.id)
        updateStmt.execute()
        if(updateStmt.getUpdateCount==0){
            insretStmt.setString(1,value.id)
            insretStmt.setDouble(2,value.temperature)
            insretStmt.execute()
        }
    }

    override def open(parameters: Configuration): Unit = {
        val conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456")
        insretStmt = conn.prepareStatement("insert into sensor_temp(id, temp) values (?,?)")
        updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
    }

    override def close(): Unit = {
        insretStmt.close()
        updateStmt.close()
        conn.close()
    }
}
