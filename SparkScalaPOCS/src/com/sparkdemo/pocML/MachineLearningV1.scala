package com.sparkdemo.pocML

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
object MachineLearningV1 {
  
    def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .appName("Test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      
      // Load the Netflix Stock CSV File, have Spark infer the data types.
val data = spark.read.option("header","true").option("inferSchema","true").csv("C:/read/USA_Housing.csv")

// What are the column names?
data.columns

// What does the Schema look like?
data.printSchema()
/*
 * describe computes statistics for numeric columns. If no columns are given, statistics for 
 * all numerical columns will be returned.
   Statistics returned: count, mean, stddev, min, max
 */
data.describe().show()
/*val colnames = data.columns
val firstrow = data.head(1)(0)
println("\n")
println("Example Data Row")
for(ind <- Range(1,colnames.length)){
  println(colnames(ind))
  println(firstrow(ind))
  println("\n")
}*/


////////////////////////////////////////////////////
//// Setting Up DataFrame for Machine Learning ////
//////////////////////////////////////////////////

// A few things we need to do before Spark can accept the data !  It needs to be in the form of two columns  // ("label","features")


// This will allow us to join multiple feature columns(array of values)
// into a single column of an array of feautre values
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}

// Step1
// Create new dataframe for ML  .Contains two columns,(1) = label,(2)= array consisting of feature values (array of all numeric columns)

// create price as label column
//val df = ( data.select(data("Price").as("label")),"Avg Area Income" ,"Avg Area House Age " ,"Avg Area Number of Rooms" , "Area Population" )

//val df = (data.select(data("Price").as("label")) , "Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Area Population"
//val df = data.select(data("Price").as("label"),"Avg Area Income");

  val df = (data.select(data("Price").as("label")),"Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Area Population")
  

// Step 2
/* An assembler, i.e vectorAssembler converts the input values to a vector ,  A vector is what the ML algorithm reads to train a model
 Set the input columns from which we are supposed to read the values
// Set the name of the column where the vector will be stored
 * */
 
val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")

// Step 3
// Use the assembler to transform our DataFrame to the two columns
//val output = assembler.transform(df).select("label","features")
}
}