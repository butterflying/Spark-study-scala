package core.demo.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://192.168.48.132:7077")
      .set("spark.driver.memory", "100m").set("spark.executor.memory", "100m").setJars(List("F:\\scala-proc\\Spark-study\\out\\wordcount.jar"))
    val sc = new SparkContext(conf)

    sc.textFile("hdfs://192.168.48.132:8020/README.txt", 1).flatMap { line => line.split(" ") }
      .map { word => (word, 1) }.reduceByKey(_ + _)
      .foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + "times."))
  }
}