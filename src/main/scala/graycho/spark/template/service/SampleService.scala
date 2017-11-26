package graycho.spark.template.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



class SampleService (vDataService: DataService) extends Serializable{

  val dataService: DataService = vDataService

  private def c(name: String) = col(name)

  def analysis(): DataFrame ={

    val df = dataService.loadData(null, null, null)

    val aggDF = aggregation(df)

    aggDF
  }

  def aggregation(ccDF: DataFrame): DataFrame = {

    var resultDF: DataFrame = null

    resultDF = ccDF.groupBy(c("key"), c("parent_seq"), c("weekofyear"), substring(c("dt"), 1, 6).as("dt"))
      .agg(
        sumDistinct(when(c("week").isin(1), 1).otherwise(0)).as("mon"),
        sumDistinct(when(c("week").isin(2), 1).otherwise(0)).as("tue"),
        sumDistinct(when(c("week").isin(3), 1).otherwise(0)).as("wed"),
        sumDistinct(when(c("week").isin(4), 1).otherwise(0)).as("thu"),
        sumDistinct(when(c("week").isin(5), 1).otherwise(0)).as("fri"),
        sumDistinct(when(c("week").isin(6), 1).otherwise(0)).as("sat"),
        sumDistinct(when(c("week").isin(7), 1).otherwise(0)).as("sun")
      )
      .groupBy(c("key"), c("parent_seq"), c("dt"))
      .agg(
        sum(c("mon")).as("mon"),
        sum(c("tue")).as("tue"),
        sum(c("wed")).as("wed"),
        sum(c("thu")).as("thu"),
        sum(c("fri")).as("fri"),
        sum(c("sat")).as("sat"),
        sum(c("sun")).as("sun")
      )
      .withColumn("week_count", (c("mon") + c("tue") + c("wed") + c("thu") + c("fri")))
      .withColumn("weekend_count", (c("sat") + c("sun")))
      .withColumn("total_count", c("week_count") + c("weekend_count"))
      .select(c("key"), c("parent_seq"), c("week_count"), c("weekend_count"), c("total_count"), c("dt"))


    resultDF
  }


}

object SampleService{
  def apply(vDataService: DataService): SampleService = new SampleService(vDataService)
}