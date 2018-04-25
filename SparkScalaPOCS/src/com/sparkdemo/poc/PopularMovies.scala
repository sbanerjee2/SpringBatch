package com.sparkdemo.poc
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object PopularMovies {
  /** Our main function where the action happens */
  
  
   def parseLine(line: String) = {
      // Split by tab 
      val fields = line.split("\t")
      // Extract  movie ID from all the colummn
      val movieID = fields(1).toInt
      movieID
      
  }
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")   
    
    // Read in each rating line  G:\SCALA-SPARK\InputFiles
    val lines = sc.textFile("file:///G:/SCALA-SPARK/InputFiles/u.data")
     val rdd = lines.map(parseLine)
     // Map (movieID) ......... to (movieID, 1) tuples
      val movietuple =  rdd.map(x => (x, 1))
    movietuple.foreach(println)
  //  689 = > (689,1)
  // (689,1)
    
    // Count up all the 1's for each movie ....   // (689,1) == > (689,87) adding all 1 s for particular key 
      //reduceByKey( (x,y) => min(x,y) different types of operation of reduceBY
    val movietupleCounts = movietuple.reduceByKey( (x, y) => x + y )
   movietupleCounts.foreach(println)
  
    
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movietupleCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Collect and print results
    val results = sortedMovies.collect()
    
    results.foreach(println)
  }
}