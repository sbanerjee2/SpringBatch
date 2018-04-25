package com.sparkdemo.poc

object DataFrameOperations  extends App{ 
  
  import org.apache.spark.sql.functions._
	import org.apache.log4j.Logger
	import org.apache.log4j.Level;
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql._
	
// 	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
		
  	val spSession = SparkCommonUtils.spSession
  	val spContext = SparkCommonUtils.spContext
  	val datadir = SparkCommonUtils.datadir
  val citidf = spSession.read.option("header","true").option("inferSchema","true").csv(datadir + "CitiGroup2006_2008")
  citidf.printSchema()
	citidf.show()
	
	import spSession.implicits._

// Grabbing all rows where a column meets a condition
  citidf.filter($"Close" === 486.2).show()

// Can also use SQL notation
//df.filter("Close > 480").show()

// Count how many results
  citidf.filter($"Close">480).count()

// Can also use SQL notation
// df.filter("Close > 480").count()

// Multiple Filters
// Note the use of triple === , this may change in the future!
//citidf.filter($"High"===484.40).show()
// Can also use SQL notation
// df.filter("High = 484.40").count()

//citidf.filter($"Close"<480 && $"High"<480).show()
// Can also use SQL notation
// df.filter("Close<480 AND High < 484.40").show()

// Collect results into a scala object (Array)
//val High484 = citidf.filter($"High"===484.40).collect()

// Operations and Useful Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
// spark -scala 
 val Highlowdiff = citidf.select($"High" - $"Low").show()

// spark SQL
// Examples of Operations
citidf.select(corr("High","Low")).show() // Pearson Correlation
}