package graycho.spark.template.service.hive

import graycho.spark.template.service.DataService
import org.apache.spark.SparkContext
import org.apache.spark.sql._


class HiveService(vSparkSession: SparkSession) extends DataService(vSparkSession: SparkSession){

  val sparkContext: SparkContext = sparkSession.sparkContext
  val hive: SQLContext = sparkSession.sqlContext

  val SAMPLE_TABLE: String = config.getString("sample.load.table")


  override def loadData(destTable: String, module: String, vWhere: Seq[String]): DataFrame = {

    val query: String =
          s"""
             select key, week, weekofyear, dt, parent_seq
              from tempTable
           """
    val rDF = hive.sql(query)
    println(s"loadData - $query")

    rDF
  }


  override def saveData(df: DataFrame, tableName: String): Boolean = {
    df.printSchema()
    println(s"saveData")

    try {
      df.write.mode(SaveMode.Append)
        .insertInto(tableName)
    } catch {
      case e: Exception => {
        println(s"saveData Exception - $e")
        return false
      }
    }
    return true
  }


  override def dropData(tableName: String, module: String, fromDt: String, toDt: String): Boolean = {

    try {
      val query: String =
        s"""
           ALTER TABLE ${tableName} drop partition (dt='${fromDt}')
         """

      hive.sql(query)
    }catch{
      case e: AnalysisException => {
        println(s"AnalysisException - $e")
      }
      return true
    }
    return true
  }


}

object HiveService {
  def apply(vSparkSession: SparkSession): HiveService = new HiveService(vSparkSession)
}
