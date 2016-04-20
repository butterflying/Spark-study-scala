package core.rdd.transform

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * 实验RDD的transform操作
  */
object RddTransform {
  def main(args: Array[String]): Unit = {
    //sortByKey
    //join
    cogroup()
  }

  /**
    * 使用sortByKey进行排序实验
    */
  def sortByKey = {
    val conf = new SparkConf().setAppName("SortByKeyTest").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = sc.parallelize(Array(Tuple2(50, "zlb"), Tuple2(45, "nn"), Tuple2(60, "lz")), 1)
    scoreList.sortByKey(false).foreach(t => println(t._1 + "\t" + t._2))

  }

  /**
    * 使用join算子对RDD进行连接
    */
  def join() = {
    val conf = new SparkConf().setAppName("JoinTest").setMaster("local")
    val sc = new SparkContext(conf)

    val stuList = Array(Tuple2(1, "zlb"), Tuple2(2, "lz"), Tuple2(3, "nn"))
    val scoreList = Array(Tuple2(1, 88), Tuple2(1, 76), Tuple2(2, 64), Tuple2(3, 76))

    val stuRDD = sc.parallelize(stuList)
    val scoreRDD = sc.parallelize(scoreList)

    stuRDD.join(scoreRDD).foreach(t => println(t._1 + "\t" + t._2._1 + "\t" + t._2._2))

  }

  /**
    * 使用cogroup算子对RDD进行连接
    * cogroup算子与join算子很类似，区别如下：
    * rdd1: <key1, value1>, <key2, value2>, <key1, value3>
    * rdd2: <key1, value4>, <key1, value5>, <key2,value6>
    * cogroup:
    * 	<key1, [[value1, value3], [value4, value5]]>
    * 	<key2, [[value2], [value6]]>
    * join:
    * 	<key1, [value1, value4]>, <key1, [value1, value5]>……
    * 相当于cogroup中的value集合又进行了一次笛卡尔积操作
    */
  def cogroup() = {
    val conf = new SparkConf().setAppName("CogroupTest").setMaster("local")
    val sc = new SparkContext(conf)

    val stuList = Array(Tuple2(1, "zlb"), Tuple2(2, "lz"), Tuple2(3, "nn"))
    val scoreList = Array(Tuple2(1, 88), Tuple2(1, 76), Tuple2(1, 91), Tuple2(2, 64), Tuple2(3, 76))

    val stuRDD = sc.parallelize(stuList)
    val scoreRDD = sc.parallelize(scoreList)

    stuRDD.cogroup(scoreRDD).foreach(t => {
      println("student id:" + t._1)
      println("student name:" + t._2._1)
      println("student scores:" + t._2._2)
    })
  }
}