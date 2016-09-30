package io.dcengines.rana

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by rana on 29/9/16.
  */
object app extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("io.dcengines.rana.datasource").load("data/")
//  df.printSchema()

  //print the data
  df.show()

  println("Application Ended...")
}
