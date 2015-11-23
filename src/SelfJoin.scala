/**
 * Created by Michael on 11/22/15.
 */

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
import java.util.{Collections, Calendar, List, StringTokenizer}

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import org.apache.spark.SparkContext._
import scala.sys.process._

object SelfJoin {
  private val exhaustive = 0

  def mapFunc(str: String): (String, String) = {
    val token = new StringTokenizer(str)
    val kMinus1 = token.nextToken()
    val kthItem2 = token.nextToken()
    return (kMinus1, kthItem2)
  }

  def main(args: Array[String]): Unit = {
    try {
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)

      lm.addLogger(logger)
      logger.setLevel(Level.INFO)

      logger.addHandler(fh)

      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val sparkConf = new SparkConf().setMaster("local[8]")



      //Lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }
      //

        sparkConf.setAppName("SelfJoin_LineageDD-" + lineage + "-" + logFile)
        .set("spark.executor.memory", "2g")

      val ctx = new SparkContext(sparkConf)

      //lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //


      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/SelfJoin.jar", "org.apache.hadoop.examples.SelfJoin", "/Users/Michael/IdeaProjects/SelfJoin/file1s", "output").!!

      //val lines = lc.textFile("../SelfJoin/file1s", 1)
      val lines = lc.textFile("../SelfJoin/file1s", 1)


      val filteredLines: RDD[String] = lines.filter(s => {
          var index: Int = 0
          index = s.lastIndexOf(",")
          if (index == -1) {
              false
          }
          else true
      })

      val maps = filteredLines.map(s => {
//          val line: String = new String
          var kMinusOne: String = new String
          var kthItem: String = new String
          var index: Int = 0
          index = s.lastIndexOf(",")
          if (index == -1) {
            //This line should never be printed out
            System.out.println("MapToPair: Input File in Wrong Format When Processing " + s)
          }
          kMinusOne = s.substring(0, index)
          kthItem = s.substring(index + 1)
          val elem: (String, String) = new (String, String)(kMinusOne, kthItem)
          elem
      })

      //var count: Long = maps.count
     // val pairedMap = new PairRDDFunctions(maps)
      //System.out.println(count);
      val groupedMap = maps.groupByKey

      //count = groupedMap.count
      //System.out.println(count);
      //val pairedGroupedmap = new PairRDDFunctions(groupedMap)
      //pairedGroupedmap.collectAsMap()
      val distinctValueMap: RDD[(String, Iterable[String])] = groupedMap.map(stringList => {
          val kthItemList: MutableList[String] = MutableList()
          for (s <- stringList._2) {
            if (!kthItemList.contains(s)) {
              kthItemList += s
            }
          }
          Collections.sort(kthItemList)
          (stringList._1, kthItemList.toIterable)
      })

      //count = distinctValueMap.count

      //System.out.println(count);
      //val pairedDistinctValueMap = new PairRDDFunctions(distinctValueMap)
      val results = distinctValueMap.flatMapValues(stringList => {
          val output: MutableList[String] = MutableList()
          val kthItemList: List[String] = stringList.toList
          for (i <- 0 until (kthItemList.size - 1)) {
            for (j <- i until kthItemList.size) {
              val outVal = kthItemList.get(i) + "," + kthItemList.get(j)
              output += outVal
            }
          }
          output.toList
      })

      println(results.count)

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/lineageResult"))

      val result = results.testGroundTruth[String, String]("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output/part-00000", mapFunc)
      var linRdd = results.getLineage()
      linRdd.collect

      linRdd = linRdd.filter{c => result.contains(c)}
      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " milliseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)


      linRdd.show.collect().foreach(s => {
        pw.append(s.toString)
        pw.append('\n')
      })
      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/lineageResult", 1)
      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging time starts at " + DeltaDebuggingStartTimestamp)

      if (exhaustive == 1) {
        val delta_debug: DD[String] = new DD[String]
        delta_debug.ddgen(lineageResult, new Test,
          new Split, lm, fh)
      } else {
        val delta_debug: DD_NonEx[String] = new DD_NonEx[String]
        delta_debug.ddgen(lineageResult, new Test, new Split, lm, fh)
      }

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " milliseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      println("Job's DONE!")
      ctx.stop()

    }
  }
}
