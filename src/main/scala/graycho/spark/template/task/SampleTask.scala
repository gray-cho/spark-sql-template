package graycho.spark.template.task

import org.apache.commons.cli.{CommandLine, OptionBuilder, Options}


object SampleTask extends SparkTask{

  var module: String = ""


  override def name(): String = "sample_task"

  override def desc(): String = s"${module}"

  override def init(options: Options): Unit = {

    LOG.info(s" ${name()} Initialize  ==============")
    OptionBuilder.hasArgs
    OptionBuilder.withDescription("Context Id ex) cc, visitor, event")
    options.addOption(OptionBuilder.create("m"))
  }

  override def parse(cmd: CommandLine): Unit = {
    LOG.info(s" ${name()} Has Args ==============")
    // option parameters
    if (cmd != null) {
      if (cmd.hasOption("m")) module = cmd.getOptionValue('m')
    }
    LOG.info(s"==> module Name : $module")
  }

  override def run(): Boolean = {



    false
  }

}

