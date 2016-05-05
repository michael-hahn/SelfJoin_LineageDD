/**
 * Created by Michael on 11/22/15.
 */

import java.io.File
import java.util.logging._
import java.util.{Calendar, Collections}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import scala.collection.JavaConversions._

object SelfJoin {
  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      //set up logger
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("SelfJoin_LineageDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
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


      //set up spark context
      val ctx = new SparkContext(sparkConf)

      //set up lineage context
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //

      //start counting the time
      val LineageStartTime = System.nanoTime()

      //Prepare for Hadoop MapReduce (for correctness test only)
//      val clw = new commandLineOperations()
//      clw.commandLineWorks()
//      //Run Hadoop to have a groundTruth
//      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/SelfJoin.jar", "org.apache.hadoop.examples.SelfJoin", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/SelfJoin/file1s", "output").!!

      //start recording lineage time
//      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      val LineageStartTime = System.nanoTime()
//      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts here
      val lines = lc.textFile("../SelfJoin/file1s_faults.data", 10)
//      logger.log(Level.INFO, "Total data count is " + lines.count)
      val selfjoin_result = lines.filter(s => {
          var index = 0
          index = s.lastIndexOf(",")
          if (index == -1) {
              false
          }
          else true
      })
      .map(s => {
          var kMinusOne = new String
          var kthItem  = new String
          var index: Int = 0
          index = s.lastIndexOf(",")
          if (index == -1) {
            //This line should never be printed out thanks to the filter operation above
            System.out.println("MapToPair: Input File in Wrong Format When Processing " + s)
          }
          kMinusOne = s.substring(0, index)
          kthItem = s.substring(index + 1)
          //println(kthItem.getClass.getSimpleName)
          (kMinusOne, kthItem)
          //elem
      })
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
      //this map marks the records that crush the program
//      .map(pair => {
//        var value = pair._1
//        var ll = pair._1.split(",")
//        for (l <- ll) {
//          if (l.equals("entryNum32763205811")) value += "*"
//        }
//        (value, pair._2)
//      })

      val out = selfjoin_result.collectWithId()

      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //To print out the result for debugging purpose
      for (tuple <- out) {
        println(tuple._1._1 + ": " + tuple._1._2 + "-" + tuple._2)
      }

//      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/lineageResult"))

      //find the index of the data that cause exception
      var list = List[Long]()
      for (o <- out) {
        val keyList = o._1._1.split(",")
        val valueList = o._1._2.split(",")
        if (keyList.contains("entryNumA") || valueList.contains("entryNumA")){
          list = o._2 :: list
        }
      }

      //print out the resulting list for debugging purposes
//      println("*************************")
//      for (l <- list) {
//        println(l)
//      }
//      println("*************************")


      var linRdd = selfjoin_result.getLineage()
      linRdd.collect

      linRdd = linRdd.filter( l => {
        //println("***" + l + "***") //debug
        list.contains(l)
      })


      linRdd = linRdd.goBackAll()
//      linRdd = linRdd.goBack()
//      linRdd.collect.foreach(println)
//      linRdd = linRdd.goBack()
//      linRdd.collect.foreach(println)

      //At this stage, technically lineage has already find all the faulty data set, we record the time
//      val lineageEndTime = System.nanoTime()
//      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " microseconds")
//      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

//      linRdd.show.collect().foreach(s => {
//        pw.append(s.toString)
//        pw.append('\n')
//      })


//      pw.close()
//      linRdd = linRdd.goNext()

      val showMeRdd = linRdd.show()


      val mappedRDD = showMeRdd.map(s => {
        var kMinusOne = new String
        var kthItem  = new String
        var index = 0
        index = s.lastIndexOf(",")
        if (index == -1) {
          //This line should never be printed out thanks to the filter operation above
          System.out.println("MapToPair: Input File in Wrong Format When Processing " + s)
        }
        kMinusOne = s.substring(0, index)
        kthItem = s.substring(index + 1)
        //println(kthItem.getClass.getSimpleName)
        (kMinusOne, kthItem)
      })

//      println("MappedRDD has " + mappedRDD.count() + " records")



      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/lineageResult", 1)
//      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/SelfJoin/file1s", 1)
//      val num = lineageResult.count()
//      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")
//
//      //Remove output before delta-debugging
//      val outputFile = new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output")
//      if (outputFile.isDirectory) {
//        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
//      }
//      outputFile.delete


//      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      val DeltaDebuggingStartTime = System.nanoTime()
//      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
//      lineageResult.cache()


//      if (exhaustive == 1) {
//        val delta_debug: DD[String] = new DD[String]
//        delta_debug.ddgen(lineageResult, new Test,
//          new Split, lm, fh)
//      } else {
        val delta_debug = new DD[(String, String)]
        val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split_v2, lm, fh)
//      }

//      val ss = returnedRDD.collect
//      ss.foreach(println)
      // linRdd.collect.foreach(println)
//      linRdd = selfjoin_result.getLineage()
//      linRdd.collect
//      linRdd = linRdd.goBack().goBack().filter(l => {
//        if(l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt){
//          println("*** => " + l)
//          true
//        }else false
//      })
//
//      linRdd = linRdd.goBackAll()
//      linRdd.collect()
//      linRdd.show()
//
//      val DeltaDebuggingEndTime = System.nanoTime()
//      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
//      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")

      val endTime = System.nanoTime()
      logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (endTime - LineageStartTime)/1000 + " microseconds")

      println("Job's DONE")
      ctx.stop()

    }
  }
}
