import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.io.PrintWriter
import org.joda.time._
import org.joda.time.DateTime
import org.joda.time.format._
import com.datastax.spark.connector._



object twitterTrending {


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


	val patternWord = "\\W|\\s|\\d"
	val patternTicker = "\\$[A-Z]+".r
	
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
		(Year, Month, Day, Hr, Min, Sec)
	}

	def getWeek(year:String, month:String, day:String)=
	{
		val date = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0)
		date.getWeekyear().toString+'-'+date.getWeekOfWeekyear().toString
	}

	def getWordSentiment(word:String)=
	{
		if (positive.contains(word)) 1 
		else if (negative.contains(word)) -1 
		else 0
	}

	def getResult(granularity:String, fileName:String)=
	{
   		val confSparkCassandra  = new SparkConf(true)
			.setAppName("Trending Twitter")
   			.set("spark.cassandra.connection.host", "54.67.105.220")

   		val sc = new SparkContext(confSparkCassandra)
	
		val hadoopConf=sc.hadoopConfiguration
        	hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        	hadoopConf.set("fs.s3n.awsAccessKeyId","")
        	hadoopConf.set("fs.s3n.awsSecretAccessKey","")
	
		// read file
		val rawUsersRDD = sc.textFile(fileName)		
		// convert tweets to JSON
		val tweets = rawUsersRDD.map( x=> parse(x))
		
		// calculate importance
		val followers = tweets.map(x=>compact(render(x \"user"\"followers_count")))
		val friends = tweets.map(x=>compact(render(x \"user"\"friends_count")))
		val importance = (followers.map(x=> x.toInt) zip friends.map(x=> x.toInt) ) map {case(a,b) => if(b>10) a/1.0/b else 1} 

		// get job description
		val job = tweets.map(x=>compact(render(x \"user"\"description")))

		// get date
		val dateString = tweets.map(x=>compact(render(x \ "created_at" )))
		val date = dateString.map(x=>getTime(x))

		// get texts
		val texts = tweets.map(x=>compact(render(x \ "text")))

		val timeStep = granularity match 
		{
	      case "YEAR" =>
	      	date map {case(a,b,c,d,e,f)=>a}
	      case "MONTH" =>
			date map {case(a,b,c,d,e,f)=>(a+'-'+b)}
	      case "WEEK"=>
		  	date map {case(a,b,c,d,e,f)=>getWeek(a,b,c)}
	      case "DAY" =>
	        date map {case(a,b,c,d,e,f)=>(a+'-'+b+'-'+c)}
	      case "HR" =>
	        date map {case(a,b,c,d,e,f)=>(a+'-'+b+'-'+c+'-'+d)}      
	      case "MIN" =>
	        date map {case(a,b,c,d,e,f)=>(a+'-'+b+'-'+c+'-'+d+'-'+e)}
	      case "SEC" =>
	      	date map {case(a,b,c,d,e,f)=>(a+'-'+b+'-'+c+'-'+d+'-'+e+'-'+f)}	      	
	    }

	    // ticker frequency
	    val mapResult = (timeStep zip texts) flatMap{case(a,b)=> (patternTicker findAllIn b).map(l=>((a,l),1))}
	   	val tickerFrequency = mapResult.reduceByKey(_+_) 
		 tickerFrequency foreach println
	   	// ticker sentiment
		val words = (timeStep zip texts) flatMap{ case(a,b)=> (b.trim().toLowerCase().split(patternWord)).map(c=>((a,b),getWordSentiment(c))) }
		val sentiment = words.reduceByKey(_+_)
		val tickerSentiment = sentiment.flatMap{ case((a,b),c) =>  (patternTicker findAllIn b).map(l=>((a,l),c)) }.reduceByKey(_+_)
		tickerSentiment foreach println
		// join both table
		val resultPair = tickerFrequency.join(tickerSentiment).map{case((a,b),(c,d))=>((a,c),(b,d))}

		//val result1 = resultPair.sortByKey(false).map{case((a,b),(c,d))=>(a,(b,c,d))}.groupByKey.map{case(a,b)=>(a->(b.toList.take(10)))}
       	
       	// sort by frequency
       	val result = resultPair.sortByKey(false).map{case((a,b),(c,d))=>(a,b,c.split('$')(1),d)}

		granularity match 
		{
			case "YEAR" =>
				val resultYear = result.map{case(date, frequency, ticker, sentiment)=> (date.toInt, frequency, ticker, sentiment)} // keyspace, table, column names
				resultYear.saveToCassandra("twittertrending", "toptrendingyear", SomeColumns("year", "frequency", "ticker", "sentiment"))			
		    case "MONTH" =>
		    	val resultMonth = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
				resultMonth.saveToCassandra("twittertrending", "toptrendingmonth", SomeColumns("year", "month", "frequency", "ticker", "sentiment"))			
			case "WEEK"=>
				val resultWeek = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
				resultWeek.saveToCassandra("twittertrending", "toptrendingweek", SomeColumns("year", "week", "frequency", "ticker", "sentiment"))			
		    case "DAY" =>
		    	val resultDay = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, frequency, ticker, sentiment)}
 			resultDay foreach println
		resultDay.saveToCassandra("twittertrending", "toptrendingday", SomeColumns("year", "month", "day", "frequency", "ticker", "sentiment"))			
		    case "HR" =>
		    	val resultHr = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, frequency, ticker, sentiment)}
 				resultHr.saveToCassandra("twittertrending", "toptrendinghour", SomeColumns("year", "month", "day", "hour", "frequency", "ticker", "sentiment"))			
		 	//case "MIN" =>
			// 	val resultMin = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, date.split('-')(4).toInt, frequency, ticker, sentiment)}
 			//	resultMin.saveToCassandra("twittertrending", "minutestock", SomeColumns("ticker", "year", "month", "day", "hour", "minute", "high", "low", "open", "close", "volume"))			
		    //case "SEC" =>
			 	//result.map{case((a,ticker),b,c,d,e,f)=> (ticker, a.split('-')(0).toInt, a.split('-')(1).toInt, a.split('-')(2).toInt, a.split('-')(3).toInt, a.split('-')(4).toInt, a.split('-')(5).toInt, b, c, d, e, f)}
				//resultCassandra.saveToCassandra("stockdata", "daystock", SomeColumns("ticker", "year", "month", "day", "hour", "minute", "second", "high", "low", "open", "close", "volume"))					      	
		}


	}

	def main(args: Array[String]) 
	{
		if (args.length != 2) 
		{
	      	System.err.println("stockFinal requires two argument")
	      	System.exit(1)
    	}
    	getResult(args(0),args(1))
	}

}
