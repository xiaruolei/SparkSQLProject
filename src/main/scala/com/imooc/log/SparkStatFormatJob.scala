package com.imooc.log

import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

        val access = spark.sparkContext.textFile("file:///Users/ruolei/imooc/data/log/10000_access.log")
//        access.take(10).foreach(println)

        access.map(line => {
            val splits = line.split(" ")
            val ip = splits(0)
            val time = splits(3) + " " + splits(4)
            val url = splits(11).replaceAll("\"", "")
            val traffic = splits(9)
//            (ip, DateUtils.parse(time), url, traffic)
            DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
        }).saveAsTextFile("file:///Users/ruolei/imooc/data/log/output/")

        spark.stop()
    }
}
