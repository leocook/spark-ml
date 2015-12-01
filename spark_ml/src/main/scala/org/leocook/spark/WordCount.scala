package org.leocook.spark

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by MX-Test11 on 2015/11/27.
 */
object WordCount {
  def main(args: Array[String]) {
    if (args.length !=1 ){
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}
