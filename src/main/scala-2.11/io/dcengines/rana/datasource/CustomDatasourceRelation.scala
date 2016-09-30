package io.dcengines.rana.datasource

import io.dcengines.rana.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
  * Created by rana on 29/9/16.
  */
class CustomDatasourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("PrunedFilterScan: buildScan called...")

    println("Filters: ")
    filters.foreach(f => println(f.toString))

    var customFilters = Map[String, List[CustomFilter]]()
    filters.foreach(f => f match {
      case EqualTo(attr, value) => {
        println("EqualTo filter is used!!" + "Attribute: " + attr + " Value: " + value);

        /**
          * as we are implementing only one filter for now, you can think that this below line doesn't mak emuch sense
          * because any attribute can be equal to one value at a time. so what's the purpose of storing the same filter
          * again if there are.
          *    but it will be useful when we have more than one filter on the same attribute. Take the below condition
          *    for example:
          *                 attr > 5 && attr < 10
          *         so for such cases, it's better to keep a list.
          * you can add some more filters in this code and try them. Here, we are implementing only equalTo filter
          * for understanding of this concept.
          */
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "equalTo")
        })
      }
      case _ => println("filter: " + f.toString + " is not implemented by us!!");
    })

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)

      val filteredData = data.map(s => if (!customFilters.isEmpty) {
        var includeInResultSet = true
        s.zipWithIndex.map {
          case (value, index) => {
            val attr = schemaFields(index).name
            val filtersList = customFilters.getOrElse(attr, List())
            if (!filtersList.isEmpty) {
              if (CustomFilter.applyFilters(filtersList, value, schema)) {
                includeInResultSet = true && includeInResultSet
              } else {
                includeInResultSet = false
              }
            }
          }
        }
        if (includeInResultSet) s else Seq()
      } else s)

      val tmp = filteredData.filter(!_.isEmpty).map(s => s.zipWithIndex.map {
        case (value, index) => {
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value.toInt == 1) "Male" else "Female"
          } else value,
            schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
        }
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })

    rows.flatMap(e => e)
  }
}
