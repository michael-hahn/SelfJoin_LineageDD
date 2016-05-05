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

object SelfJoinV2 {
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
			val sparkConf = new SparkConf().setMaster("local[6]")
			sparkConf.setAppName("SelfJoin_LineageDD")
				.set("spark.executor.memory", "2g")

			//set up lineage
			var lineage = true
			var logFile = "test_log"

			//set up spark context
			val ctx = new SparkContext(sparkConf)

			//set up lineage context
			val lc = new LineageContext(ctx)
			lc.setCaptureLineage(lineage)

			//start recording lineage time
			val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val LineageStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

			//spark program starts here
			val lines = lc.textFile("SelfJoin_file1s.data", 10)
			//      logger.log(Level.INFO, "Total data count is " + lines.count)
			val selfjoin_result = lines.filter(s => {
				var index: Int = 0
				index = s.lastIndexOf(",")
				if (index == -1) {
					false
				}
				else true
			})
				.map(s => {
				var kMinusOne: String = new String
				var kthItem: String = new String
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
				.map(pair => {
				var value = pair._1
				var ll = pair._1.split(",")
				for (l <- ll) {
					if (l.equals("entryNum32763205811")) value += "*"
				}
				(value, pair._2)
			})

			val out = selfjoin_result.collectWithId()
			println(">>>>>>>>>>>>>  First Run Done  <<<<<<<<<<<<<<<")
			//stop capturing lineage information
			lc.setCaptureLineage(false)
			Thread.sleep(1000)

			//To print out the result for debugging purpose
			//for (tuple <- out) {
			//println(tuple._1._1 + ": " + tuple._1._2 + "-" + tuple._2)
			//}

			//find the index of the data that cause exception
			var list = List[Long]()
			for (o <- out) {
				val checkPoint = o._1._1.substring(o._1._1.length - 1)
				if (checkPoint.equals("*")) {
					list = o._2 :: list
				}
			}

			var linRdd = selfjoin_result.getLineage()
			linRdd.collect

			linRdd = linRdd.filter(l => {
				//println("***" + l + "***") //debug
				list.contains(l)
			})


			linRdd = linRdd.goBackAll()

			println(">>>>>>>>>>>>>  Go Back All Done  <<<<<<<<<<<<<<<")
			//At this stage, technically lineage has already find all the faulty data set, we record the time
			val lineageEndTime = System.nanoTime()
			val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
			logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

			//linRdd = linRdd.goNext()

			val showMeRdd = linRdd.show().toRDD
			showMeRdd.collect().foreach(println)
			println(">>>>>>>>>>>>>  Go Next Done  <<<<<<<<<<<<<<<")

			val mappedRDD = showMeRdd.map(s => {
				(s, 0L)
			})
			//mappedRDD.cache()
			//Remove output before delta-debugging
			//			val outputFile = new File("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output")
			//			if (outputFile.isDirectory) {
			//				for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
			//			}
			//			outputFile.delete
			//

			val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val DeltaDebuggingStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

			val delta_debug = new DD_NonEx[String, Long]
			val returnedRDD = delta_debug.ddgen(mappedRDD, new TestV2, new SplitV2, lm, fh)

			val ss = returnedRDD.collect
			ss.foreach(println)
			//			linRdd = selfjoin_result.getLineage()
			//			linRdd.collect
			//			linRdd = linRdd.goBack().goBack().filter(l => {
			//				if (l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt) {
			//					println("*** => " + l)
			//					true
			//				} else false
			//			})
			//			linRdd = linRdd.goBackAll()
			//			linRdd.collect()
			//			linRdd.show()
			val DeltaDebuggingEndTime = System.nanoTime()
			val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
			logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " microseconds")
			println("Job's DONE! goNext does not work!")
			ctx.stop()

		}
	}
}
