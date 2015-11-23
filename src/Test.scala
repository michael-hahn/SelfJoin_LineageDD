/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._





class Test extends userTest[String] {

  def usrTest(inputRDD: JavaRDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)
    var returnValue = false
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/SelfJoin/file2"
    val file = new File(fileName)

    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/SelfJoin.jar", "org.apache.hadoop.examples.SelfJoin", fileName, "output").!!

    var truthList:Map[String, String] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/SelfJoin_LineageDD/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val kMinusOne: String= token.nextToken()
      val kthItems:String = token.nextToken()
      truthList += (kMinusOne -> kthItems)
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator()
    while (itr.hasNext) {
      val tupVal = itr.next()
      val kListName = tupVal._1
      val kItems = tupVal._2
      //logger.log(Level.INFO, "Output: " + binName + " : " + num)
      if (truthList.contains(kListName)) {
        if (kItems != truthList.get(kListName).get){
          returnValue = true
        }
      } else returnValue = true
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
    return returnValue
  }
}
