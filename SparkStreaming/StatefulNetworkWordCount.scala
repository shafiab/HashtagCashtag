import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}

object StatefulNetworkWordCount {
  def updateFunc(values: Seq[Int], runningCount: Option[Int]):
      Option[Int] = {
    val newCount = values.foldLeft(0)(_ + _)
    val oldCount = runningCount.getOrElse(0)
    Some(newCount + oldCount)
  }

  def main(args: Array[String]) {
     val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")
     val sc = new StreamingContext(sparkConf, Seconds(1))

    // This is crucial to the stateful word counting. Otherwise you will
    // hit an exception that the checkpoint directory is unset and the
    // network input will just get queued.
    sc.checkpoint("log")
    val dstream = sc.socketTextStream("localhost", 1234)
	dstream.print
    val lineCounts = dstream.map(x => (x, 1)).reduceByKey(_ + _)
    val runningLineCounts = lineCounts.updateStateByKey[Int](updateFunc _)
    lineCounts.print
    runningLineCounts.print

    val wordCounts = dstream.flatMap(_.split(" ")).map(x => (x,1))
        .reduceByKey(_ + _)
    val runningCounts = wordCounts.updateStateByKey[Int](updateFunc _)
    println(wordCounts)
    
    println(runningCounts)

    sc.start()
    sc.awaitTermination()
  }
}
