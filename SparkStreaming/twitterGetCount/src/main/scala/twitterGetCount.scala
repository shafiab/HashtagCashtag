import java.util.Properties
import kafka.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.joda.time._
import org.joda.time.DateTime
import org.joda.time.format._
import java.io.PrintWriter
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object twitterGetCount
{
	// sentiment - currently hardcoded
	// need to change to read from a file
	val positive = Set(
		"upgrade",
		"upgraded",
		"long",
		"buy",
		"buying",
		"growth",
		"good",
		"gained",
		"well",
		"great",
		"nice",
		"top",
		"support",
		"update",
		"strong",
		"bullish",
		"bull",
		"highs",
		"win",
		"positive",
		"profits",
		"bonus",
		"potential",
		"success",
		"winner",
		"winning",
		"good")


	val negative =Set(
		"downgraded",
		"bears",
		"bear",
		"bearish",
		"volatile",
		"short",
		"sell",
		"selling",
		"forget",
		"down",
		"resistance",
		"sold",
		"sellers",
		"negative",
		"selling",
		"blowout",
		"losses",
		"war",
		"lost",
		"loser")
	
	// get sentiment of a word
	def getWordSentiment(word:String)=
	{
		if (positive.contains(word)) 1 
		else if (negative.contains(word)) -1 
		else 0
	}

    val patternWord = "\\W|\\s|\\d"
    val patternTicker = "\\$[A-Z]+".r
    
	// get index of a month
    def getMonth(month:String)=
    { 
        val m = month.toUpperCase() match
        {
            case "JAN"=>1
            case "FEB"=>2
            case "MAR"=>3
            case "APR"=>4
            case "MAY"=>5
            case "JUN"=>6
            case "JUL"=>7
            case "AUG"=>8
            case "SEP"=>9
            case "OCT"=>10
            case "NOV"=>11
            case "DEC"=>12
    
        }
        m.toString
    }

	// return date-time from string
    def getTime(dateString:String) = 
    {
        val str = dateString.split(' ')
        val Month = getMonth(str(1))
        val Day = str(2)
        val Year = str(5).split('"')(0)
        val time = str(3).split(':')
        val Hr = time(0)
        val Min = time(1)
        val Sec = time(2)

        ( Year, Month, Day, Hr, Min, Sec)
    }

	// return week index
    def getWeek(year:String, month:String, day:String)=
    {
        val date = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0)
        date.getWeekyear().toString+'-'+date.getWeekOfWeekyear().toString
    }
    
	// update function for updateStateByKey
    def updateFunc(values: Seq[(Int,Int)], runningCount: Option[(Int,Int)]):
      Option[(Int,Int)] = {
        val newCount1 = values.map(x=>x._1).sum 
        val newCount2 = values.map(x=>x._2).sum 

        val (oldCount1,oldCount2) = runningCount.getOrElse((0,0))
       Some((newCount1 + oldCount1, newCount2 + oldCount2)) 
    }

	// main function
    def getResult(granularity:String)=
    {
		// create configuration
        val confSparkCassandra  = new SparkConf(true)
                    .setAppName("Twitter Streaming Series")
                    .set("spark.cassandra.connection.host", "54.67.105.220")
    
		// create spark streaming context
        val ssc = new StreamingContext(confSparkCassandra, Seconds(60))
		
		// create checkpoint
        ssc.checkpoint("twitter count")

        // create Kakfa stream
        // Set up the input DStream to read from Kafka (in parallel)
        val zkQuorum = "localhost:2181"
        val group  = "SparkStreaming"
        val inputTopic = "twitterStream"
        val topicMap =  Map(inputTopic -> 1)
        val numPartitionsOfInputTopic = 1
  
        // create a DStream from Kafka
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

		// perform json parsing
        val tweets = lines.map( x=> parse(x))
        
        // get date
        val date = tweets.map(x=> ( getTime(compact(render(x \ "created_at" ))), compact(render(x \ "text")) )  )
			.map{case((a,b,c,d,e,f),text)=>(a,b,c,d,e,f,text)}
    

        // get texts
        val texts = tweets.map(x=>compact(render(x \ "text")))
		
		// map based on time granularity
        val timeStep = granularity match 
        {
          case "YEAR" =>
            date map {case(a,b,c,d,e,f,text)=>(a,text)}
          case "MONTH" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b,text)}
          case "WEEK"=>
            date map {case(a,b,c,d,e,f,text)=>(getWeek(a,b,c),text)}
          case "DAY" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c,text)}
          case "HR" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d,text)}      
          case "MIN" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d+'-'+e,text)}
          case "SEC" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d+'-'+e+'-'+f,text)}         
        }

        // ticker frequency
        val mapResult = timeStep flatMap{ case(a,b)=> (patternTicker findAllIn b).toList.map(l=>((a,l),1))}

        // ticker sentiment
        val words = timeStep flatMap{ case(a,b)=> (b.trim().toLowerCase().split(patternWord)).map(c=>((a,b),getWordSentiment(c))) }
        val sentiment = words.reduceByKey(_+_)
        val tickerSentiment = sentiment.flatMap{ case((a,b),c) =>  (patternTicker findAllIn b).toList.map(l=>((a,l),c)) } 
        
        // join ticker frequency and sentiment
        val pair1 = mapResult join tickerSentiment
    
        // main operation, update state by Key-time granularity
        val resultPair = pair1.updateStateByKey[(Int,Int)](updateFunc _)

        // flatten the Dstream
        val result = resultPair.map{case((date,ticker),(frequency,sentiment))=>(date,frequency,ticker.split('$')(1),sentiment)}


        // save it to cassandra database
        val keySpace = "twitterseries"
        granularity match 
        {
            case "YEAR" =>
                val resultYear = result.map{case(date, frequency, ticker, sentiment)=> (date.toInt, frequency, ticker, sentiment)} // keyspace, table, column names
                resultYear.saveToCassandra(keySpace, "trendingyear", SomeColumns("year", "frequency", "ticker", "sentiment"))            
            case "MONTH" =>
                val resultMonth = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
                resultMonth.saveToCassandra(keySpace, "trendingmonth", SomeColumns("year", "month", "frequency", "ticker", "sentiment"))         
            case "WEEK"=>
                val resultWeek = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
                resultWeek.saveToCassandra(keySpace, "trendingweek", SomeColumns("year", "week", "frequency", "ticker", "sentiment"))            
            case "DAY" =>
                val resultDay = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, frequency, ticker, sentiment)}
                resultDay.saveToCassandra(keySpace, "trendingday", SomeColumns("year", "month", "day", "frequency", "ticker", "sentiment"))          
            case "HR" =>
                val resultHr = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, frequency, ticker, sentiment)}
                resultHr.saveToCassandra(keySpace, "trendinghour", SomeColumns("year", "month", "day", "hour", "frequency", "ticker", "sentiment"))          
            case "MIN" =>
                val resultMin = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, date.split('-')(4).toInt, frequency, ticker, sentiment)}
                resultMin.saveToCassandra(keySpace, "trendingminute", SomeColumns( "year", "month", "day", "hour", "minute", "frequency", "ticker", "sentiment"))            
            //case "SEC" =>
            //result.map{case((a,ticker),b,c,d,e,f)=> (ticker, a.split('-')(0).toInt, a.split('-')(1).toInt, a.split('-')(2).toInt, a.split('-')(3).toInt, a.split('-')(4).toInt, a.split('-')(5).toInt, b, c, d, e, f)}
            //resultCassandra.saveToCassandra("stockdata", "daystock", SomeColumns("ticker", "year", "month", "day", "hour", "minute", "second", "high", "low", "open", "close", "volume"))                         
        }

        ssc.start()
        ssc.awaitTermination()

}


    def main(args: Array[String]) 
    {

        if (args.length != 1) 
        {
            System.err.println("twitterGetCount requires one argument")
            System.exit(1)
        }

        getResult(args(0))
    }

  
}





