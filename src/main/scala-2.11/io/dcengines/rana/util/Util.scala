package io.dcengines.rana.util

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

/**
  * Created by rana on 30/9/16.
  */
object Util {
  def castTo(value : String, dataType : DataType) = {
    dataType match {
      case _ : IntegerType => value.toInt
      case _ : LongType => value.toLong
      case _ : StringType => value
    }
  }
}
