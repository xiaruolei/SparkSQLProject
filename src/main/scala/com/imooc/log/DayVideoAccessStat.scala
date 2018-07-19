package com.imooc.log

/**
  * visits of course per day entity class
  * @param day
  * @param cmsId
  * @param times
  */
case class DayVideoAccessStat (day: String, cmsId: Long, times: Long)
