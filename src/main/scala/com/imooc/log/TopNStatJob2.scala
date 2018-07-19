package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Stat Top N Spark Job
  */
object TopNStatJob2 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("SparkStatCleanJob")
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
                .master("local[2]").getOrCreate()

        val accessDF = spark.read.format("parquet").load("file:///Users/ruolei/imooc/data/clean/")
//        accessDF.printSchema()
//        accessDF.show(false)

        val day = "20170511"

        import spark.implicits._
        val commonDF = accessDF.filter($"day" === day and $"cmsType" === "video")
        commonDF.cache()

        StatDAO.deleteData(day)

        //top N most popular courses
        videoAccessTopNStat(spark, commonDF)

        //top N most popular courses ranking by region
        cityAccessTopNStat(spark, commonDF)

        //top N most popular courses ranking by traffics
        videoTrafficsTopNStat(spark, commonDF)

        commonDF.unpersist(true)

        spark.stop()
    }

    /**
      * top N most popular courses
      * @param spark
      * @param commonDF
      */
    def videoAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
        /**
          * using DataFrame to collect statistics
          */
        import spark.implicits._
        val videoAccessTopNDF = commonDF.groupBy("day", "cmsId")
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
      * @param commonDF
      */
    def cityAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
        import spark.implicits._
        val cityAccessTopNDF = commonDF.groupBy("day", "city", "cmsId")
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
      * @param commonDF
      */
    def videoTrafficsTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
        import spark.implicits._
        val videoTrafficsTopNDF = commonDF.groupBy("day", "cmsId")
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
