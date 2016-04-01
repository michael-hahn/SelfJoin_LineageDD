/**
 * Created by Michael on 11/13/15.
 */

import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.delta.DeltaWorkflowManager
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.io.Source
import org.apache.spark.SparkContext._

import java.io.File
import java.io._
import scala.Tuple2
import java.util.{Collections, Calendar, StringTokenizer}

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList





class Test extends userTest[(String, String)] with Serializable {

  def usrTest(inputRDD: RDD[(String, String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass, which returns false
    var returnValue = false

    /*the rest of the code is for correctness test
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/SelfJoin/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/SelfJoin.jar", "org.apache.hadoop.examples.SelfJoin", "-m", "3", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")


    var truthList:Map[String, String] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val kMinusOne: String = token.nextToken()
      val kthItems:String = token.nextToken()
      truthList += (kMinusOne -> kthItems)
      //logger.log(Level.INFO, "Truth: " + kMinusOne + ":" + kthItems)
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }

    val itr = output.iterator()
    while (itr.hasNext) {
      val tupVal = itr.next()
      val kListName = tupVal._1
      val kItems = tupVal._2
      //logger.log(Level.INFO, "Output: " + kListName + ":" + kItems)
      if (truthList.contains(kListName)) {
        if (kItems != truthList(kListName)){
          returnValue = true
        } else {
          truthList = truthList - kListName
        }
      } else returnValue = true
    }
    if (!truthList.isEmpty) {
      returnValue = true
    }

    val outputFile = new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    */

    val finalRdd= inputRDD.groupByKey()
         //.reduceByKey(_+ ";" + _)
         .map(stringList1 => {
      val kthItemList: MutableList[String] =  MutableList()
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
         //this map marks the records that crush the program
         .map(pair => {
      var value = pair._1
      var ll = pair._1.split(",")
      for (l <- ll) {
        if (l.equals("entryNum32763205811")) value += "*"
      }
      (value, pair._2)
    })
    val out = finalRdd.collect()
    for (o <- out) {
      println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    return returnValue
  }
}
