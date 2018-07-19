package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}


object MySQLUtils {
    def getConnection() = {
        DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=root&useUnicode=true&characterEncoding=utf8")
    }

    /**
      * release database connection and other resources
      * @param connection
      * @param pstmt
      */
    def release(connection: Connection, pstmt: PreparedStatement) = {
        try {
            if (pstmt != null) {
                pstmt.close()
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            if (connection != null) {
                connection.close()
            }
        }
    }

    def main(args: Array[String]): Unit = {
        println(getConnection())
    }

}
