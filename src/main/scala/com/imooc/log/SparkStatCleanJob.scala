package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * using spark to clean data
  */
object SparkStatCleanJob {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("SparkStatCleanJob")
                .config("spark.sql.parquet.compression.codec", "gzip").master("local[2]").getOrCreate()

        val accessRDD = spark.sparkContext.textFile("file:///Users/ruolei/imooc/data/access.log")
//        accessRDD.take(10).foreach(println)

        //RDD ===> DF
        val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

//        accessDF.printSchema()
//        accessDF.show(false)

        accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("file:///Users/ruolei/imooc/data/clean2")

        spark.stop()
    }
}
