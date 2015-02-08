import java.util.Properties
import kafka.producer._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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

object twitterTopTrending
{	
	// sentiments, hardcoded for now
	// will change it layer to read from a file
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
	
    // function to get the sentiment of a  word
	def getWordSentiment(word:String)=
	{
		if (positive.contains(word)) 1 
		else if (negative.contains(word)) -1 
		else 0
	}

    val patternWord = "\\W|\\s|\\d"
    val patternTicker = "\\$[A-Z]+".r
    

    // get month index
    // no good substitute in joda-time
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

	
    // get time from date string
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

	
    // get week index for week calculation
    def getWeek(year:String, month:String, day:String)=
    {
        val date = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0)
        date.getWeekyear().toString+'-'+date.getWeekOfWeekyear().toString
    }
    
	
    // main function
    def getResult()=
    {

        // create spark configuration
        val confSparkCassandra  = new SparkConf(true)
                .setAppName("Twitter Streaming Top Trending")
                .set("spark.cassandra.connection.host", "54.67.105.220")

        // create streaming context        
        val ssc = new StreamingContext(confSparkCassandra, Seconds(5))
        
        // create checkpoint
        ssc.checkpoint("Top trending hour")


        // create Kakfa stream
        // Set up the input DStream to read from Kafka (in parallel)
        val zkQuorum = "localhost:2181"
        val group  = "SparkStreamingTopTrending"
        val inputTopic = "twitterStream"
        val topicMap =  Map(inputTopic -> 1)
        val numPartitionsOfInputTopic = 1
  
        // create a DStream from Kafka
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

        // parse JSON
        val tweets = lines.map( x=> parse(x))
        

        // get date
        val date = tweets.map(x=> ( getTime(compact(render(x \ "created_at" ))), compact(render(x \ "text")) )  )
    			.map{case((a,b,c,d,e,f),text)=>(a,b,c,d,e,f,text)}

        // get texts
        val texts = tweets.map(x=>compact(render(x \ "text")))


        // select granularity
        val granularity = "MIN" // currently hard-coded to min
		// however code is generic for any granularity
		
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


        // find ticker frequency
        val mapResult = timeStep flatMap{ case(a,b)=> (patternTicker findAllIn b).toList.map(l=>((a,l),1))}


        // find ticker sentiment
        val words = timeStep flatMap{ case(a,b)=> 		(b.trim().toLowerCase().split(patternWord)).map(c=>((a,b),getWordSentiment(c))) }
        val sentiment = words.reduceByKey(_+_)
        val tickerSentiment = sentiment.flatMap{ case((a,b),c) =>  (patternTicker findAllIn b).toList.map(l=>((a,l),c)) } 

        // join ticker frequency with sentiment
        val pair1 = mapResult join tickerSentiment

        // remove date as key
        val lastWindow1 = pair1.map{ case((date,ticker),(frequency,sentiment)) => (ticker, (frequency,sentiment)) }

       
	   // calculate last 10 minutes data every 5 seconds
	    val lastWindow2 = lastWindow1.reduceByKeyAndWindow((a:(Int,Int),b:(Int,Int))=>(a._1+b._1,a._2+b._2), Seconds(600), Seconds(5))
        val resultSorted = lastWindow2.map{case(ticker,(frequency, sentiment))=>(frequency,(ticker,sentiment))}
                        .transform(x=>x.sortByKey(false))
                        .map{ case(frequency,(ticker,sentiment))=>(ticker.split('$')(1), frequency, sentiment) }

	   // add an index on sorted result
	   val resultIndexed = resultSorted.transform(_.zipWithIndex).map(_.swap)
	
	   // take top 5
	   val resultTopFive = resultIndexed.filter{case(a,(b,c,d)) => (a>=0 && a<5)}
	
	   // add a timestamp
	   val resultTimeStamped = resultTopFive.map{case(a,(b,c,d))=>(System.currentTimeMillis, a,b,c,d)}

	   // save to cassandra
	   resultTimeStamped.saveToCassandra("twittertrendingstreaming", "toptrending30min", SomeColumns("timestamp", "id", "ticker", "frequency", "sentiment"))

        ssc.start()
        ssc.awaitTermination()

  }


    def main(args: Array[String]) 
    {
        getResult()
    }

  
}





