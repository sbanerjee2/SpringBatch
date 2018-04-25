package com.sparkdemo.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 

   /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "RatingsCounter")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("file:///G:/SCALA-SPARK/InputFiles/book.txt")
    
    // (1) Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
      words.foreach(println)
   
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // (2) Create tuple ( word ,1 )  
    val tuplewords = lowercaseWords.map(x => (x, 1))
   // tuplewords.foreach(println)
     
    // Count of the occurrences of each word , for a word (key) add the 1s
    val wordCounts = tuplewords.reduceByKey( (x,y) => x + y )
    wordCounts.foreach(println)
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}
