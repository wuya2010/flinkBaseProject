package flink_bulid

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @author kylinWang
  * @data 2020/6/30 23:43
  *
  */
object flink_wordcount {
  def main(args: Array[String]): Unit = {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val inputPath = "flinkTest.txt"
      val inputDataSet = env.readTextFile(inputPath)

    //要添加这个依赖
      import org.apache.flink.streaming.api.scala._

      val wordCount = inputDataSet.flatMap(_.split(" "))
        .map((_, 1)).groupBy(0).sum(1)


      wordCount.print()
  }
}
