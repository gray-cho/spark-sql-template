package graycho.spark.template

import java.util.Date

import graycho.spark.template.task.{SampleTask, SparkTask}
import org.slf4j.LoggerFactory



object Main {

  val LOG = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit ={
    val tasks: List[SparkTask] = List(SampleTask)
    val taskName: String = args(0)

    if(taskName == null || "".equals(taskName)){
      LOG.error("TaskName is required.. you can use one of.")
      for (task <- tasks) {
        LOG.error(s" - ${task.name()}")
      }
      LOG.error("Default TaskName Used: backup_cc")
    }


    var result: Boolean = false
    for(task <- tasks){
      LOG.info(s"check task name - ${task.name()} , ${taskName}")
      if (task.name().equalsIgnoreCase(taskName)) {
        val now = new Date
        LOG.info(s"============== ${taskName} Task Start!!  ==============")
        result = task.execute(args)
        LOG.info(s"============== ${taskName} - result : ${result} Task End!! (take-{} min)==============", (System.currentTimeMillis - now.getTime) / 60000)
      }
    }

    if(!result){
      LOG.error(s"Error In Task ${taskName}")
      throw new Exception(s"Error in Task : ${taskName}")
    }

    LOG.info("Batch end...")

  }

}
