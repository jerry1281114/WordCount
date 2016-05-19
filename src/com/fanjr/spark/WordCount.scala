package com.fanjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by fanjinrui on 2016/5/19.
  */
object WordCount {
  def main( args: Array[String]){

    /**
      * 一、定义 conf，设置appname及运行方式
     */
    val conf = new SparkConf()
    conf.setAppName("wow,my first spark app")
    conf.setMaster("local")

    /**
      * 二、定义sc，并配置input
      */
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://hadoop-2.6.4//README.txt")

    /**
     * 三、拆分input
     */
    val words = lines.flatMap{_.split(" ")}

    /**
      * 四、map操作
      */
    val pairs = words.map{(_,1)}

    /**
      * 五、reduce合并
      */
    val wordCounts = pairs.reduceByKey(_+_)

    /**
      *六、println
      */
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))

    /**
      * 七、stop
      */
    sc.stop()

  }

}
