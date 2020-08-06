package com.tlh.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/4 11:50
 * @Version 1.0
 */
object WorldCount {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        //1.从文件中读取数据
        val inputPath = "D:\\IdeaProjects\\BigDataWSR\\FlinkTutorial\\src\\main\\resources\\hello.txt"
        val inputDS: DataSet[String] = env.readTextFile(inputPath)

        //2.分词,聚合,sum
        import org.apache.flink.api.scala._
        val wordCountDS: AggregateDataSet[(String,Int)] = inputDS.flatMap(_.split(" "))
          .map((_, 1))
          .groupBy(0)
          .sum(1)

        //打印输出
        wordCountDS.print()
    }
}
