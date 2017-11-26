package graycho.spark.template.task

import graycho.spark.template.service.{SampleCSVService, SampleService}
import graycho.spark.template.{IntentTest, SparkInitTest}


class SampleTaskTest extends IntentTest with SparkInitTest{

  val dataService = SampleCSVService(session)

  test("SampleTest시작"){
    val service = SampleService(dataService)
    val result = service.analysis()



  }

}
