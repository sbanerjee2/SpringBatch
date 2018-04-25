package com.sparkdemo.poc
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager , ResultSet}
object JBBCUtil {
  def main(args: Array[String]) {
    
    val spSession = SparkCommonUtils.spSession
  	val spContext = SparkCommonUtils.spContext
  	
  	val url = "jdbc:mysql://localhost:3306/person_example"
  	val driverClass = "com.mysql.jdbc.Driver"
  	val userName ="root"
  	val password ="root"
  	Class.forName(driverClass).newInstance()
  	 val myRDD = new JdbcRDD(spContext , () => DriverManager.getConnection(url,userName,password), 
  	    "SELECT * FROM  person_v1 limit ? , ?", 3,5,2 , r => r.getString("location") + "," +r.getString("name"))
    myRDD
    myRDD.foreach(println)
  }
  
}