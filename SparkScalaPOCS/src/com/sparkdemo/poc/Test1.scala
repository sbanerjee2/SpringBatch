package com.sparkdemo.poc
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object Test1 {
  
    def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")   
   val hello: String = "Hola!"                     //> hello  : String = Hola!

  println(hello)           
}
}