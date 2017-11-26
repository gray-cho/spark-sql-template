package graycho.spark.template

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


trait SparkInit {

  def name(): String
  def desc(): String

  val config: Config = ConfigFactory.load("application")
  protected var session: SparkSession = _
  protected var spark: SparkContext = _
  protected var sql: SQLContext = _

  protected def initSpark(): Unit = {
    val builder = SparkSession.builder()
      .appName(s"${name()} - ${desc()}")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // when do not use partition by in df, query will be "insert ~ partition by ~" with out this option. that processed in map stage only (each map will open file)
      // with this option, query will be "insert ~ distributed by ~". that processed reduce by key so, use map and reduce stage (file will open in reducer)
      .config("hive.exec.max.dynamic.partitions", "100000")
      .config("hive.exec.max.dynamic.partitions.pernode", "100000")
      .config("hive.auto.convert.join.noconditionaltask", "true")
      .config("hive.auto.convert.join.noconditionaltask.size", "300000000")

    if ("local".equalsIgnoreCase(config.getString("spark.env"))){
      builder.master(config.getString("spark.default_master"))
    }

    session = builder.getOrCreate()
    spark = session.sparkContext
    sql = session.sqlContext

  }


}
