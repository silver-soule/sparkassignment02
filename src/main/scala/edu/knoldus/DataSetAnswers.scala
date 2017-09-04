package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by Neelaksh on 31/8/17.
  */

case class TeamInfo(HomeTeam:String,AwayTeam:String,FTHG:Int,FTAG:Int,FTR:String)

object DataSetAnswers extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Spark Demo")
  val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  val spark = sparkSession
  val csvDataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/gitika/IdeaProjects/sparkassignment02/src/main/resources/footballdata.csv")

  import sparkSession.implicits._

  //----------------------------- dataset #1---------------------------

  val homeInfoDataSet = csvDataFrame.select(col("HomeTeam"), col("AwayTeam"), col("FTHG"), col("FTAG"), col("FTR")).as[TeamInfo]

  //----------------------------- dataset #2---------------------------

  homeInfoDataSet.map(_.HomeTeam).union(homeInfoDataSet.map(_.AwayTeam)).groupBy(col("value") as "team").count().show()

  //----------------------------- dataset #3---------------------------

  case class MinimalData(homeTeam:String,FTR:String,awayTeam:String)

  val winner: (String,String,String) => String = (home: String,away:String,result:String) => {
  if(result == "H")
    home
  else if(result == "A")
    away
  else
    null
  }

  val winnerUDF = udf(winner)
  homeInfoDataSet.map(data=> MinimalData(data.HomeTeam,data.FTR,data.AwayTeam)).withColumn("winner",winnerUDF(col("HomeTeam"),col("awayTeam"),col("FTR")))
    .groupBy("winner").count().na.drop().orderBy(desc("count")).show(10)
}
