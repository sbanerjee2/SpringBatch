package com.sparkdemo.poc
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
object PopularMoviesNicer {
  
    def parseLine(line: String) = {
      // Split by tab 
      val fields = line.split("\t")
      // Extract  movie ID from all the colummn
      val movieID = fields(1).toInt
      movieID
      
  }
  
   /** Define a method to Load up a Map of movie IDs to movie names. */
    def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("G://SCALA-SPARK/InputFiles/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')  // get (# movieId , # movieName)
       if (fields.length > 1) {
         // movieNames.put("1" ,Toy Story (1995)")
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")  
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("file:///G:/SCALA-SPARK/InputFiles/u.data")
    val rdd = lines.map(parseLine) //get movie ID
     // Map (movieID) to (movieID, 1) tuples to know ratings
      val movietuple =  rdd.map(x => (x, 1))
  //  movietuple.foreach(println)
  //  689 = > (689,1)
  // (689,1)
    
    // Count up all the 1's for each movie ....   // (689,1) == > (689,87) adding all 1 s for particular key 
    val movietupleCounts = movietuple.reduceByKey( (x, y) => x + y )
   // movietupleCounts.foreach(println)
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movietupleCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    sortedMovies.foreach(println)
    // Fold in the movie names from the broadcast variable -- nameDict.value(movieID)
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
 //  results.foreach(println)
  }
}