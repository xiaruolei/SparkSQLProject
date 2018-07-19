package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Stat Top N Spark Job, running on YARN
  */
object TopNStatJobYARN {
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println("Usage: TopNStatJobYARN <inputPath> <day>")
            System.exit(1)
        }

        val Array(inputPath, day) = args

        val spark = SparkSession.builder()
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
                .getOrCreate()

        val accessDF = spark.read.format("parquet").load(inputPath)

        StatDAO.deleteData(day)

        //top N most popular courses
        videoAccessTopNStat(spark, accessDF, day)

        //top N most popular courses ranking by region
        cityAccessTopNStat(spark, accessDF, day)

        //top N most popular courses ranking by traffics
        videoTrafficsTopNStat(spark, accessDF, day)

        spark.stop()
    }

    /**
      * top N most popular courses
      * @param spark
      * @param accessDF
      */
    def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
        /**
          * using DataFrame to collect statistics
          */
        import spark.implicits._
        val videoAccessTopNDF = accessDF.filter($"day" === day and $"cmsType" === "video").groupBy("day", "cmsId")
        .agg(count("cmsId").as("times")).orderBy($"times".desc)

//        videoAccessTopNDF.show(false)

        /**
          * using sql to collect statistics
          */
//        accessDF.createOrReplaceTempView("access_logs")
//        val videoAccessTopNDF  = spark.sql("select day, cmsId, count(1) as times " +
//                "from access_logs " +
//                "where day = '20170511' and cmsType = 'video' " +
//                "group by day, cmsId " +
//                "order by times desc")
//        videoAccessTopNDF.show(false)

        /**
          * write statistic results to mysql
          */
        try {
            videoAccessTopNDF.foreachPartition(partitionOfRecords => {
                var list = new ListBuffer[DayVideoAccessStat]

                partitionOfRecords.foreach(info => {
                    val day = info.getAs[String]("day")
                    val cmsId = info.getAs[Long]("cmsId")
                    val times = info.getAs[Long]("times")

                    list.append(DayVideoAccessStat(day, cmsId, times))
                })

                StatDAO.insertDayVideoAccessTopN(list)
            })
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }

    /**
      * top N most popular courses ranking by region
      * @param spark
      * @param accessDF
      */
    def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
        import spark.implicits._
        val cityAccessTopNDF = accessDF.filter($"day" === day and $"cmsType" === "video").groupBy("day", "city", "cmsId")
                .agg(count("cmsId").as("times"))

        //cityAccessTopNDF.show(false)

        //Window function in Spark SQL
        val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
            cityAccessTopNDF("city"),
            cityAccessTopNDF("cmsId"),
            cityAccessTopNDF("times"),
            row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
                    .orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
        ).filter("times_rank <= 3")
        top3DF.show(false)

        /**
          * write statistic results to mysql
          */

        try {
            top3DF.foreachPartition(partitionOfRecords => {
                val list = new ListBuffer[DayCityVideoAccessStat]

                partitionOfRecords.foreach(info => {
                    val day = info.getAs[String]("day")
                    val cmsId = info.getAs[Long]("cmsId")
                    val city = info.getAs[String]("city")
                    val times = info.getAs[Long]("times")
                    val timesRank = info.getAs[Int]("times_rank")

                    list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))

                })

                StatDAO.insertDayCityVideoAccessTopN(list)
            })
        } catch {
            case e: Exception => e.printStackTrace()
        }

    }

    /**
      * top N most popular courses ranking by traffic
      * @param spark
      * @param accessDF
      */
    def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
        import spark.implicits._
        val videoTrafficsTopNDF = accessDF.filter($"day" === day and $"cmsType" === "video").groupBy("day", "cmsId")
                .agg(sum("traffic").as("traffics"))
                .orderBy($"traffics".desc)
//                .show(false)

        /**
          * write statistic results to mysql
          */
        try {
            videoTrafficsTopNDF.foreachPartition(partitionOfRecords => {
                var list = new ListBuffer[DayVideoTrafficsStat]

                partitionOfRecords.foreach(info => {
                    val day = info.getAs[String]("day")
                    val cmsId = info.getAs[Long]("cmsId")
                    val traffics = info.getAs[Long]("traffics")

                    list.append(DayVideoTrafficsStat(day, cmsId, traffics))
                })

                StatDAO.insertDayVideoTrafficsAccessTopN(list)
            })
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }
}
