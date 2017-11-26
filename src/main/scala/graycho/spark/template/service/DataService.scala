package graycho.spark.template.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class DataService(vSparkSession: SparkSession) {

  val config: Config = ConfigFactory.load("service")
  val sparkSession: SparkSession = vSparkSession

  val conditionPath: String = "/data/backup_condition.csv"


  /**
    * load Data
    * @return
    */

  def loadData(destTable: String, module: String, where: Seq[String]): DataFrame


  /**
    * Save Data
    * @param df
    * @param tableName
    * @return
    */

  def saveData(df: DataFrame, tableName: String): Boolean


  /**
    * Drop Data
    * @param tableName
    * @return
    */

  def dropData(tableName: String, module: String, fromDt: String, toDt: String): Boolean



}

object DataService extends Serializable{

  val CC_SCHEMA = StructType( Array(
    StructField("key", StringType, nullable = true),
    StructField("enter_time", StringType, nullable = true),
    StructField("exit_time", StringType, nullable = true),
    StructField("dwelltime", StringType, nullable = true),
    StructField("sensing_area", StringType, nullable = true),
    StructField("tag_seq", StringType, nullable = true),
    StructField("week", IntegerType, nullable = true),
    StructField("weekofyear", IntegerType, nullable = true),
    StructField("dt", StringType, nullable = true),
    StructField("parent_seq", StringType, nullable = true)
  ))
}