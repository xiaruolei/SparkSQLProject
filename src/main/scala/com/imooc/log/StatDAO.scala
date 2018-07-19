package com.imooc.log

import java.sql
import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * DAO operations for each dimension
  */
object StatDAO {
    /**
      * batch save DayVideoAccessStat to database
      * @param list
      */
    def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {
        var connection: Connection = null
        var pstmt: PreparedStatement = null

        try {
            connection = MySQLUtils.getConnection()

            connection.setAutoCommit(false) //设置手动提交

            val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?, ?, ?)"
            pstmt = connection.prepareStatement(sql)

            for (ele <- list) {
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsId)
                pstmt.setLong(3, ele.times)

                pstmt.addBatch()
            }

            pstmt.executeBatch()
            connection.commit() //手动提交
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            MySQLUtils.release(connection, pstmt)
        }
    }

    /**
      * batch save DayCityVideoAccessStat to database
      * @param list
      */
    def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]) = {
        var connection: Connection = null
        var pstmt: PreparedStatement = null

        try {
            connection = MySQLUtils.getConnection()

            //设置手动提交
            connection.setAutoCommit(false)

            val sql = "insert into day_video_city_access_topn_stat(day, cms_id, city, times, times_rank) values(?, ?, ?, ?, ?)"
            pstmt = connection.prepareStatement(sql)

            for (ele <- list) {
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsId)
                pstmt.setString(3, ele.city)
                pstmt.setLong(4, ele.times)
                pstmt.setInt(5, ele.timesRank)

                pstmt.addBatch()
            }

            pstmt.executeBatch()
            connection.commit()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            MySQLUtils.release(connection, pstmt)
        }
    }

    /**
      * batch save DayVideoTrafficsAccessStat to database
      * @param list
      */
    def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]) = {
        var connection: Connection = null
        var pstmt: PreparedStatement = null

        try {
            connection = MySQLUtils.getConnection()

            //设置手动提交
            connection.setAutoCommit(false)

            val sql = "insert into day_video_traffics_topn_stat(day, cms_id, traffics) values(?, ?, ?)"
            pstmt = connection.prepareStatement(sql)

            for (ele <- list) {
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsId)
                pstmt.setLong(3, ele.traffics)

                pstmt.addBatch()
            }

            pstmt.executeBatch()
            connection.commit()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            MySQLUtils.release(connection, pstmt)
        }
    }

    /**
      * delete data of specific day
      * @param day
      */
    def deleteData(day: String): Unit = {
        val tables = Array("day_video_access_topn_stat", "day_video_city_access_topn_stat", "day_video_traffics_topn_stat")

        var connection: Connection = null
        var pstmt: PreparedStatement = null

        try {
            connection = MySQLUtils.getConnection()

            for (table <- tables) {
                val deleteSQL = s"delete from $table where day = ?"
                pstmt = connection.prepareStatement(deleteSQL)
                pstmt.setString(1, day)

                pstmt.executeUpdate()
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            MySQLUtils.release(connection, pstmt)
        }
    }
}