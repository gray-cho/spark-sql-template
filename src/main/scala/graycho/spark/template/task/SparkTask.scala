package graycho.spark.template.task

import java.io.{PrintWriter, StringWriter}

import graycho.spark.template.SparkInit
import org.apache.commons.cli._
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory


trait SparkTask extends SparkInit {

  val LOG = LoggerFactory.getLogger(classOf[SparkContext])

  var options = new Options
  OptionBuilder.withArgName("property=value")
  OptionBuilder.hasArgs
  OptionBuilder.withValueSeparator
  OptionBuilder.withDescription("use value for given property")
  options.addOption(OptionBuilder.create("m"))

  val formatter: HelpFormatter = new HelpFormatter

  def init(options: Options): Unit

  def usage(): Unit = formatter.printHelp(name, options)

  def parse(cmd: CommandLine)

  def run(): Boolean

  def execute(args: Array[String]): Boolean = {
    try {

      init(options)
      val parser: CommandLineParser = new GnuParser
      val cmd = parser.parse(options, args)
      parse(cmd)
      initSpark()

    } catch {
      case e: Exception => {
        LOG.error(getStackTraceAsString(e))
      }
        usage
        return false

    }
    run
  }

  // Utilities
  protected def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}
