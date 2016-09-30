package io.dcengines.rana.datasource

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by rana on 29/9/16.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new CustomDatasourceRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", "./output/") //can throw an exception/error, it's just for this tutorial
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " already exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val formatName = parameters.getOrElse("format", "customFormat")
    formatName match {
      case "customFormat" => saveAsCustomFormat(data, path, mode)
      case "json" => saveAsJson(data, path, mode)
      case _ => throw new IllegalArgumentException(formatName + " is not supported!!!")
    }
    createRelation(sqlContext, parameters, data.schema)
  }

  private def saveAsJson(data : DataFrame, path : String, mode: SaveMode): Unit = {
    /**
      * Here, I am using the dataframe's Api for storing it as json.
      * you can have your own apis and ways for saving!!
      */
    data.write.mode(mode).json(path)
  }

  private def saveAsCustomFormat(data : DataFrame, path : String, mode: SaveMode): Unit = {
    /**
      * Here, I am  going to save this as simple text file which has values separated by "|".
      * But you can have your own way to store without any restriction.
      */
    val customFormatRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString("|")
    })
    customFormatRDD.saveAsTextFile(path)
  }
}
