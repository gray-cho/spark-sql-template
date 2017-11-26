package graycho.spark.template

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory


trait IntentTest extends FunSuite with BeforeAndAfter {
  val LOG = LoggerFactory.getLogger(classOf[IntentTest])
}
