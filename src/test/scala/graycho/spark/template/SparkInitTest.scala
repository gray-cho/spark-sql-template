package graycho.spark.template

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession


trait SparkInitTest extends Serializable {

  val config = ConfigFactory.load("config/application")
  val session = SparkSession.builder().master("local").appName("TEST").config("spark.sql.parquet.compression.codec","uncompressed").getOrCreate()
  val spark = session.sparkContext
  val sql = session.sqlContext

}
