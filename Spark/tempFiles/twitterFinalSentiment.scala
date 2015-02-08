import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.io._



object twitterFinalSentiment {
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

	def getTime(dateString:String) = 
	{
		val str = dateString.split(' ')
		val Month = str(1)
		val Day = str(2)
		val Year = str(5).split('"')(0)
		val time = str(3).split(':')
		val Hr = time(0)
		val Min = time(1)
		val Sec = time(2)
		(Year, Month, Day, Hr, Min, Sec)
	}
		// top 10 trending stocks
/*	def findTopTen(texts : org.apache.spark.rdd.RDD[String]) =
	{
		val w = texts.flatMap(l => patternTicker findAllIn l).map(word=>(word,1))
		val counts = w.reduceByKey(_+_)
		val sortedCounts = counts.map{case(a,b)=>(b,a)}.sortByKey(false).map{case(a,b)=>(b,a)}
		//val result = sortedCounts.take(10).map(x=>(time,x))
		sortedCounts.take(10)
	}*/

	def getResult(granularity:String)=
	{

		val conf = new SparkConf().setAppName("Twitter Top 10")
   		val sc = new SparkContext(conf)
		// read file
		val rawUsersRDD = sc.textFile("twitterData.txt")
		//val rawUsersRDD = sc.textFile("twitter.txt")

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

		val timeStep = granularity match {
	      case "YEAR" =>
	      	date map {case(a,b,c,d,e,f)=>a}
	      case "MONTH" =>
			date map {case(a,b,c,d,e,f)=>(a+b)}
	      case "DAY" =>
	        date map {case(a,b,c,d,e,f)=>(a+b+c)}
	      case "HR" =>
	        date map {case(a,b,c,d,e,f)=>(a+b+c+d)}      
	      case "MIN" =>
	        date map {case(a,b,c,d,e,f)=>(a+b+c+d+e)}
	      case "SEC" =>
	      	date map {case(a,b,c,d,e,f)=>(a+b+c+d+e+f)}	      	
	    }

	    val mapResult = (timeStep zip texts) flatMap{case(a,b)=> (patternTicker findAllIn b).map(l=>((a,l),1))}
	   	val reduceResult = mapResult.reduceByKey(_+_).map{case ((a,b),c)=>((a,c),b)}.sortByKey(false).map{case((a,b),c)=>(a,(b,c))}
	   	//val result = reduceResult.groupByKey.map{case(a,b)=>a->b.toList.take(5)}

	   	// map texts to work
		val words = (timeStep zip texts) flatMap {case(b,a)=>a.trim().toLowerCase().split(patternWord) map (c=>(b,a,c))} 

//		val sentiment = words map {case(a,c,b)=> ((a,c),if (positive.contains(b)) 1 else if (negative.contains(b)) -1 else 0))}
		val sentimentMap = words map {case(a,c,b)=> ((a,c),(if (positive.contains(b)) 1 else if (negative.contains(b)) -1 else 0))}
		val sentiment = sentimentMap reduceByKey(_+_) map {case((a,b),c)=>((a,c),b)}
		val sentimentStock = sentiment flatMap{case((a,b),c)=> (patternTicker findAllIn c).map(l=>((a,b),l))}
		val negativeStock = sentimentStock sortByKey() map{case((a,b),c)=>(a,(b,c))} groupByKey() map {case(a,b)=>a->b.toList.take(5)}
		val positiveStock = sentimentStock sortByKey(false) map{case((a,b),c)=>(a,(b,c))} groupByKey() map {case(a,b)=>a->b.toList.take(5)}


	    //(timeStep zip texts) flatMap{case(a,b)=> (patternTicker findAllIn b).map(l=>((a,l),1)}

		//val distinctTimeStep = timeStep.distinct()

		//val textMinute = (timeStep zip texts) filter { case(x,l) => (x==minute) } map{case(a,b)=>b};findTopTen(textMinute)}
		//val result = distinctMinutes.(minute=>{val textMinute = (minutes zip texts) filter { case(x,l) => (x==minute) } map{case(a,b)=>b};findTopTen(textMinute)})

		//println("TOP 10")
		//val result = distinctTimeStep.collect().map(y=>findTopTen(y, (timeStep zip texts) filter { case(x,l) => (x==y) } map{case(a,b)=>b}))

		val fileName = "resultPositive"+granularity+".txt"
		val S = new PrintWriter(fileName)
		val resP = positiveStock.collect()

 		for (i<- 0 to resP.length-1)
      	{
      		S.println(resP(i))
      	}
      	S.close()


		val fileName1 = "resultNegative"+granularity+".txt"
		val T = new PrintWriter(fileName1)
      	val resN = negativeStock.collect()

 		for (i<- 0 to resN.length-1)
      	{
      		T.println(resN(i))
      	}
      	T.close()

		// var step =""
		// for (step<-distinctTimeStep.collect())
		// {
		// 	println(step)
		// 	val textTimeStep = (timeStep zip texts) filter { case(x,l) => (x==step) } map{case(a,b)=>b}
		// 	findTopTen(textTimeStep) foreach print
		// 	println
		// }
	}
		//println("SENTIMETNS")

	//	val distinctTimeStep1 = timeStep.distinct()

	// 	step=""
	// 	for (step<-distinctTimeStep1.collect())
	// 	{
	// 		println(step)
	// 		var textTimeStep = (timeStep zip texts) filter { case(x,l) => (x==step) } map{case(a,b)=>b}
	// //		var textMinute = (minutes zip texts) filter { case(x,l) => (x=="2015Jan2116") } map{case(a,b)=>b}

	// 		val sentiments = getSentiments(textTimeStep) 
	// 		println
	// 		println("positive")
	// 			sentiments._1 foreach print
	// 		println
	// 		println("negative")
	// 			sentiments._2 foreach print
	// 	}

	//}




/*	def getLine(s:String) ={
		val words = s.trim().toLowerCase().split(patternWord)
		val sentiment = words.filter(x=>positive.contains(x)).length -  words.filter(x=>negative.contains(x)).length 
		val tickers = patternTicker findAllIn s
		val y = tickers.map(x=>(x,sentiment,1))
		y
	}

	// top 10 stocks with positive and negative sentiments
	def getSentiments(texts : org.apache.spark.rdd.RDD[String]) = 
	{
		val res = texts.flatMap(x=>getLine(x))
		val sentimentCount = res.map({case (x,y,z)=>(x,y)}).reduceByKey(_+_)	
		val sentimentsPositive = sentimentCount.map{case(a,b)=>(b,a)}.sortByKey(false).map{case(a,b)=>(b,a)}
		val sentimentsNegative= sentimentCount.map{case(a,b)=>(b,a)}.sortByKey().map{case(a,b)=>(b,a)}
		(sentimentsPositive.take(10), sentimentsNegative.take(10))
	}

*/


	def main(args: Array[String]) 
	{
		if (args.length != 1) 
		{
	      	System.err.println("twitterFinal requires one argument")
	      	System.exit(1)
    	}
    	getResult(args(0))
	}

}
