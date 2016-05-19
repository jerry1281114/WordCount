package com.fanjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by fanjinrui on 2016/5/19.
  */
object WordCount {
  def main( args: Array[String]){

    /**

     */
    val conf = new SparkConf()
    conf.setAppName("wow,my first spark app")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://hadoop-2.6.4//README.txt")

    val words = lines.flatMap{_.split(" ")}
    val pairs = words.map{(_,1)}

    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))

    sc.stop()




















  }

}
