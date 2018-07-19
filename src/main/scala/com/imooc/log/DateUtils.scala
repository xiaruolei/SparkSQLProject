package com.imooc.log

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
    //input date format
    //10/Nov/2016:00:01:02 +0800
    val YYYYMMDD_TIME_FORMATE = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)

    //target date format
    val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    /**
      * get time format：yyyy-MM-dd HH:mm:ss
      * @param time
      */
    def parse(time: String) = {
        TARGET_FORMAT.format(new Date(getTime(time)))
    }

    /**
      * get input date：long type
      * @param time
      */
    def getTime(time: String) = {
        try {
            YYYYMMDD_TIME_FORMATE.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
        } catch {
            case e: Exception => {
                0L
            }
        }
    }

    def main(args: Array[String]): Unit = {
        println(parse("[10/Nov/2016:00:01:02 +0800]"))
    }
}
