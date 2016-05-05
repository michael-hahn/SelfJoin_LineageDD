/**
 * Created by Michael on 11/13/15.
 */
import java.io._
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.{Collections, Calendar, StringTokenizer}

//remove if not needed
import scala.collection.JavaConversions._

import java.util.{Collections,StringTokenizer}
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.MutableList
import scala.util.control.Breaks._


import java.io._
import java.util.Collections
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList

class Test extends userTest[(String, String)] with Serializable {
  var num = 0
  def usrTest(inputRDD: RDD[(String, String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    val finalRDD = inputRDD
      .groupByKey()
      //.reduceByKey(_+ ";" + _)
      .map(stringList1 => {
          val kthItemList: MutableList[String] = MutableList()
          for (s <- stringList1._2) {
            if (!kthItemList.contains(s)) {
              kthItemList += s
            }
          }
          Collections.sort(kthItemList)
          (stringList1._1, kthItemList.toList)
      })
      .filter(pair => {
        if (pair._2.size < 2) false
        else true
      })
      .flatMap(stringList => {
        val output: MutableList[(String, String)] = MutableList()
        val kthItemList: List[String] = stringList._2.toList
        for (i <- 0 until (kthItemList.size - 1)) {
          for (j <- (i + 1) until kthItemList.size) {
            val outVal = kthItemList.get(i) + "," + kthItemList.get(j)
            output += Tuple2(stringList._1, outVal)
          }
        }
        output.toList
      })

    val start = System.nanoTime

    val out = finalRDD.collect()
    logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
    num = num + 1
    logger.log(Level.INFO, "TestRuns : " + num)
    println(s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

    for (o <- out) {
      val keyList = o._1.split(",")
      val valueList = o._2.split(",")
      if (keyList.contains("entryNumA") || valueList.contains("entryNumA")){
        returnValue = true
      }
    }
    returnValue
  }

  //FOR LOCAL COMPUTATION TEST WILL ALWAYS PASS
  def usrTest(inputRDD: Array[(String,String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    val start = System.nanoTime
    val finalRDD = inputRDD
      .groupBy(_._1)
      //.reduceByKey(_+ ";" + _)
      .map(stringList1 => {
          val kthItemList: MutableList[String] = MutableList()
          for (s <- stringList1._2) {
            if (!kthItemList.contains(s._2)) {
              kthItemList += s._2
            }
          }
          Collections.sort(kthItemList)
          (stringList1._1, kthItemList.toList)
      })
      .filter(pair => {
        if (pair._2.size < 2) false
        else true
      })
      .flatMap(stringList => {
        val output: MutableList[(String, String)] = MutableList()
        val kthItemList: List[String] = stringList._2.toList
        for (i <- 0 until (kthItemList.size - 1)) {
          for (j <- (i + 1) until kthItemList.size) {
            val outVal = kthItemList.get(i) + "," + kthItemList.get(j)
            output += Tuple2(stringList._1, outVal)
          }
        }
        output.toList
      })

    val out = finalRDD
    logger.log(Level.INFO, "LTimeTest : " + (System.nanoTime() - start) / 1000)
    num = num +1
    logger.log(Level.INFO, "LTestRuns : " + num)
    println(s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      val keyList = o._1.split(",")
      val valueList = o._2.split(",")
      if (keyList.contains("entryNumA") || valueList.contains("entryNumA")){
        returnValue = true
      }
    }
    returnValue

  }
}

