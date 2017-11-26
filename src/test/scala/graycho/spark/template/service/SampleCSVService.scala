package graycho.spark.template.service

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}


class SampleCSVService(vSparkSession: SparkSession) extends DataService(vSparkSession){
  val sparkContext: SparkContext = vSparkSession.sparkContext
  val sqlContext: SQLContext = vSparkSession.sqlContext

  val ccDataPath = "/data/comm_cc.csv"
  val ccSavePath = "/tmp/comm_cc_result"

  /**
    * load Data
    *
    * @return
    */
  override def loadData(destTable: String, module: String, where: Seq[String]): DataFrame = {

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(DataService.CC_SCHEMA)
      .load(getClass.getResource(ccDataPath).toString)

    df

  }

  /**
    * Save Data
    *
    * @param df
    * @param tableName
    * @return
    */
override def saveData(df: DataFrame, tableName: String): Boolean = {
  df.persist()
  df.coalesce(1).write
    .format("com.databricks.spark.csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save(ccSavePath)

  true
}

  /**
    * Drop Data
    *
    * @param tableName
    * @return
    */
  override def dropData(tableName: String, module: String, fromDt: String, toDt: String): Boolean = ???
}

object SampleCSVService{
  def apply(vSparkSession: SparkSession): SampleCSVService = new SampleCSVService(vSparkSession)
}
