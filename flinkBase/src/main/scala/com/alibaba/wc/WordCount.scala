package com.alibaba.wc

import org.apache.flink.api.scala._

/**
  *
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  *  2019/10/18 11:23
  */

// 批处理word count程序

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "F:\\03. Codes\\03-实时分析\\02-Flink\\01-IDEA\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到word，然后再按word做分组聚合
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
