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

//	DateTimeZone zone = DateTimeZone.forID("Asia/Tokyo");
	val zoneFrom = DateTimeZone.forID("Europe/Oslo");
	val  zoneTo = DateTimeZone.forID("US/Pacific");

	def getTime(dateString:String) = 
	{
		val str = dateString.split(' ')
		val date = str(0).split('-')
		val time = str(1).split(':')
		
		val Year = date(0)
		val Month = date(1)
		val Day = date(2)
				
		val Hr = time(0)
		val Min = time(1)
		val Sec = time(2)
		val dt = new DateTime(Year.toInt, Month.toInt, Day.toInt, Hr.toInt, Min.toInt, Sec.toInt, 0, zoneFrom);
		val newDate = dt.toDateTime(zoneTo)
		val ds1 =  DateTimeFormat.forPattern("yyyy MM dd HH mm ss").print(newDate)
		val ds = ds1.split(' ')
	//	println(dateString+' '+newDate)
	//	val str1 = newDate.getYear()+','+newDate.getMonthOfYear()+','+newDate.DayOfMonth()+' '+newDate.getHourOfDay()+':'+newDate.getMinuteOfHour()+':'+newDate.getSecondOfMinute()
	// println(dateString+' '+dateString1)
	(ds(0), ds(1), ds(2), ds(3), ds(4), ds(5))
	}

	def computeResult(granularity:String, fileName:String)
	{
		
		val conf = new SparkConf().setAppName("Stock Data")
   		val sc = new SparkContext(conf)
	
		val rawUsersRDD = sc.textFile(fileName)
		val data = rawUsersRDD.map(line => line.split(",").map(elem=>elem.trim))
		val Dat1 = data.map(line=> (line(1),line(2),line(3),line(4)))
		val Dat = Dat1 map {case(a,b,c,d) => (getTime(b)->(a,c,d))}  

		val Data = granularity match 
		{
	      case "YEAR" =>
	      	Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a,x,y,z)}//{case(a,b,c,d,e,f)=>a}
	      case "MONTH" =>
			Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b,x,y,z)}//{case(a,b,c,d,e,f)=>(a+b)}
	      case "DAY" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c,x,y,z)}//{case(a,b,c,d,e,f)=>(a+b+c)}
	      case "HR" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d,x,y,z)}//{case(a,b,c,d,e,f)=>(a+b+c+d)}      
	      case "MIN" =>
	        Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d+e,x,y,z)}//{case(a,b,c,d,e,f)=>(a+b+c+d+e)}
	      case "SEC" =>
	      	Dat map {case((a,b,c,d,e,f),(x,y,z))=>(a+b+c+d+e+f,x,y,z)}//{case(a,b,c,d,e,f)=>(a+b+c+d+e+f)}	      	
	    }

		val maxx =Data map {case(a,b,c,d)=>((a,b),c)} reduceByKey{(x,y)=> if(x>y) x else y}
		val minn =Data map {case(a,b,c,d)=>((a,b),c)} reduceByKey{(x,y)=> if(x<y) x else y}
		val open = Data map {case(a,b,c,d)=>((a,b),c)} groupByKey() map{case(a,b)=>(a->b.toList.head)}
		val close = Data map {case(a,b,c,d)=>((a,b),c)} groupByKey() map{case(a,b)=>(a->b.toList.last)}
		val volume =  Data map {case(a,b,c,d)=>((a,b),d.toDouble)}  reduceByKey(_+_) 

		val resultTemp1 =  maxx join (minn) 
		val resultTemp2 = open join (close) 
		val resultTemp = resultTemp1 join (resultTemp2) join (volume)
		//val result = resultTemp map {case(a,(((b, c), (d, e), f)))=>(a,b,c,d,e,f)}
		val result = resultTemp map {case(a,(( (b, c), (d, e)), f ))=>(a,b,c,d,e,f)}

		result foreach println
	}

	def main(args: Array[String]) 
	{

		if (args.length != 2) 
		{
	      	System.err.println("stockFinal requires two argument")
	      	System.exit(1)
    	}
//		println(new DateTime)
//		println(DateTime.now)
    	//val myDate = new DateTime(2010, 3, 1, 0, 0, 0, 0)
		//println(myDate.toString("yyyy-MM-dd")
    	computeResult(args(0),args(1))
	}




}
