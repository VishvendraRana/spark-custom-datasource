package io.dcengines.rana.datasource

import io.dcengines.rana.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types._

/**
  * Created by rana on 29/9/16.
  */
class CustomDatasourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with TableScan with PrunedScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("id", IntegerType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", StringType, true) ::
        StructField("salary", LongType, true) ::
        StructField("expenses", LongType, true) :: Nil
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) => {
          val colName = schemaFields(index).name
          Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
            schemaFields(index).dataType)
        }
      })

      tmp.map(s => Row.fromSeq(s))
    })

    rows.flatMap(e => e)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    println("PrunedScan: buildScan called...")

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) => {
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
                                        schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
        }
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })

    rows.flatMap(e => e)
  }
}
