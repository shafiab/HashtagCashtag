import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//import org.json4s.JsonDSL._
import java.io.PrintWriter
//import com.github.nscala_time.time.Imports._
import org.joda.time._
import org.joda.time.DateTime
//import org.joda.time.format.DateTimeFormatter
//import org.joda.time.format.DateTimeFormat
//import org.joda.time.DateTime
//import org.joda.time.DateTimeZone
//import org.joda.time.TimeZoneTable
import org.joda.time.format._

object stockFinal
{

	// create DateTimeZone for OSLO and PST
	val zoneFrom = DateTimeZone.forID("Europe/Oslo");
	val  zoneTo = DateTimeZone.forID("US/Pacific");

	// get time and convert from OSLO to PST
	def getTime(dateString:String) = 
	{
		// get date and time from the time string
		val str = dateString.split(' ')
		val date = str(0).split('-')
		val time = str(1).split(':')
		
		val Year = date(0)
		val Month = date(1)
		val Day = date(2)
				
		val Hr = time(0)
		val Min = time(1)
		val Sec = time(2)

		// create a DateTime object	
		val dt = new DateTime(Year.toInt, Month.toInt, Day.toInt, Hr.toInt, Min.toInt, Sec.toInt, 0, zoneFrom);
		// convert from OSLO time to PST
		val newDate = dt.toDateTime(zoneTo)
		val ds1 =  DateTimeFormat.forPattern("yyyy MM dd HH mm ss").print(newDate)
		val ds = ds1.split(' ')
	
		// return PST result
		(ds(0), ds(1), ds(2), ds(3), ds(4), ds(5))
	}

	// create week from year, month and day info
	def getWeek(year:String, month:String, day:String)=
	{
		val datePST = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0, zoneTo)
		year+'-'+datePST.getWeekOfWeekyear().toString
	}

	def computeResult(granularity:String, fileName:String)
	{
		
		// get spark context
		val conf = new SparkConf().setAppName("Stock Data")
   		val sc = new SparkContext(conf)
	
		// read input file
		val rawUsersRDD = sc.textFile(fileName)

		// process input line and convert it to key, column value format
		val data = rawUsersRDD.map(line => line.split(",").map(elem=>elem.trim))
		val Dat1 = data.map(line=> (line(1),line(2),line(3),line(4)))
		// convert time from OSLO to PST
		val Dat = Dat1 map {case(a,b,c,d) => (getTime(b)->(a,c,d))}  

		// create time key for the time granularity
		val Data = granularity match 
		{
	      case "YEAR" =>
	      	Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a,x,y,z)}
	      case "MONTH" =>
			Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b,x,y,z)}
		  case "WEEK"=>
		  	Dat map {case((a,b,c,d,e,f),(x,y,z))=>(getWeek(a,b,c),x,y,z)}
	      case "DAY" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c,x,y,z)}
	      case "HR" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d,x,y,z)}
	      case "MIN" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d+e,x,y,z)}
	      case "SEC" =>
	      	Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d+e+f,x,y,z)}
	    }

	    // find maximum value
		val maxx = Data.map{case(a,b,c,d)=>((a,b),c)} 
					   .reduceByKey{(x,y)=> if(x>y) x else y}

		// find minimum value
		val minn =Data.map{case(a,b,c,d)=>((a,b),c)} 
					  .reduceByKey{(x,y)=> if(x<y) x else y}

		// find opening value			   
		val opening = Data.map{case(a,b,c,d)=>((a,b),c)}
						  .groupByKey() 
						  .map{case(a,b)=>(a->b.toList.head)}

		// find closing value
		val closing = Data.map{case(a,b,c,d)=>((a,b),c)}
						  .groupByKey()
						  .map{case(a,b)=>(a->b.toList.last)}

		// find volume				   
		val volume =  Data.map{case(a,b,c,d)=>((a,b),d.toDouble)}
						  .reduceByKey(_+_) 

		// create result				   	
		val resultTemp1 =  maxx join (minn)
		val resultTemp2 = opening join (closing) 
		val resultTemp = resultTemp1 join (resultTemp2) join (volume)		
		val result = resultTemp map {case(a,(( (b, c), (d, e)), f ))=>(a,b,c,d,e,f)}

		result foreach println
	}

	def main(args: Array[String]) 
	{
		// check number of input arguments
		if (args.length != 2) 
		{
	      	System.err.println("stockFinal requires two argument")
	      	System.exit(1)
    	}
    	computeResult(args(0),args(1))
	}
	
}
