package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Neelaksh on 30/8/17.
  */
object DataFrameAnswers extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Spark Demo")
  val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  val spark = sparkSession


  //----------------------------- dataframe #1---------------------------
  val csvDataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/gitika/IdeaProjects/sparkassignment02/src/main/resources/footballdata.csv").cache()


  //----------------------------- dataframe #2---------------------------

  val homeTeams = csvDataFrame.withColumn("homegames",lit(1)).select(col("HomeTeam"),col("homegames"))

  val awayTeams = csvDataFrame.withColumn("result",lit(0)).select(col("AwayTeam"),col("result"))

  homeTeams.union(awayTeams).groupBy(col("HomeTeam") as "team").sum("homegames").show()


//-------------------------------------OR------------------------------------

  csvDataFrame.createOrReplaceTempView("matches")

  val homeTeamWin = spark.sql("select table1.HomeTeam,table2.AwayTeam from matches as table1 RIGHT JOIN (Select distinct(AwayTeam) from matches) as table2 on table1.HomeTeam = table2.AwayTeam ")

  val teamName: (String,String) => String = (home: String,away:String) => {
    if(home == null)
      away
    else if(away == null)
      home
    else
      home
  }

  val count: (String,String) => Int = (home: String,_:String) => {
    if(home == null) 0 else 1
  }
  val countUDF = udf(count)
  val teamNameUDF = udf(teamName)
  homeTeamWin.withColumn("team",teamNameUDF(col("homeTeam"),col("AwayTeam"))).withColumn("homegames",countUDF(col("HomeTeam"),col("AwayTeam"))).groupBy("team").sum().show()


  //----------------------------- dataframe #3---------------------------

  val homeTeamWinLoss: (String) => Int = (_: String) match {
    case "H" => 1
    case "D"|"A" => 0
  }
  val homeWinLossUDF = udf(homeTeamWinLoss)
  val homeTeamWithWin = csvDataFrame.select(col("HomeTeam"),col("FTR")).withColumn("result",homeWinLossUDF(col("FTR"))).withColumnRenamed("HomeTeam","team").select(col("team"),col("result"))

  val awayTeamWinLoss: (String) => Int = (_: String) match {
    case "A" => 1
    case "H"|"D" => 0
  }

  val awayWinLossUDF = udf(awayTeamWinLoss)
  val awayTeamWithWin = csvDataFrame.select(col("AwayTeam"),col("FTR")).withColumn("result",awayWinLossUDF(col("FTR"))).withColumnRenamed("AwayTeam","team").select(col("team"),col("result"))

  homeTeamWithWin.union(awayTeamWithWin).groupBy("team").avg("result").sort(desc("avg(result)")).show(10)

}
