/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.qubole.sparklens.timespan

import java.util

import com.google.gson.{Gson, JsonObject}
import com.qubole.sparklens.common.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable

/*
This keeps track of data per stage
*/

class StageTimeSpan(val stageID: Int, numberOfTasks: Long) extends TimeSpan {
  var stageMetrics  = new AggregateMetrics()
  var tempTaskTimes = new mutable.ListBuffer[( Long, Long, Long)]
  var minTaskLaunchTime = Long.MaxValue
  var maxTaskFinishTime = 0L
  var parentStageIDs:Seq[Int] = null

  // we keep execution time of each task
  var taskExecutionTimes  = Array.emptyIntArray
  var taskPeakMemoryUsage = Array.emptyLongArray

  def updateAggregateTaskMetrics (taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
    stageMetrics.update(taskMetrics, taskInfo)
  }

  def setParentStageIDs(parentIDs: Seq[Int]): Unit = {
    parentStageIDs = parentIDs
  }

  def updateTasks(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = {
    if (taskInfo != null && taskMetrics != null) {
      tempTaskTimes += ((taskInfo.taskId, taskMetrics.executorRunTime, taskMetrics.peakExecutionMemory))
      if (taskInfo.launchTime < minTaskLaunchTime) {
        minTaskLaunchTime = taskInfo.launchTime
      }
      if (taskInfo.finishTime > maxTaskFinishTime) {
        maxTaskFinishTime = taskInfo.finishTime
      }
    }
  }

  def finalUpdate(): Unit = {
    //min time for stage is when its tasks started not when it is submitted
    setStartTime(minTaskLaunchTime)
    setEndTime(maxTaskFinishTime)

    taskExecutionTimes = new Array[Int](tempTaskTimes.size)

    var currentIndex = 0
    tempTaskTimes.sortWith(( left, right)  => left._1 < right._1)
      .foreach( x => {
        taskExecutionTimes( currentIndex) = x._2.toInt
        currentIndex += 1
      })

    val countPeakMemoryUsage = {
      if (tempTaskTimes.size > 64) {
         64
      }else {
        tempTaskTimes.size
      }
    }

    taskPeakMemoryUsage = tempTaskTimes
      .map( x => x._3)
      .sortWith( (a, b) => a > b)
      .take(countPeakMemoryUsage).toArray

    /*
    Clean the tempTaskTimes. We don't want to keep all this objects hanging around for
    long time
     */
    tempTaskTimes.clear()
  }

  override def getJavaMap(): util.Map[String, _ <: Any] = {
    import scala.collection.JavaConverters._
    (Map(
      "stageID" -> stageID,
      "numberOfTasks" -> numberOfTasks,
      "stageMetrics" -> stageMetrics.getJavaMap(),
      "minTaskLaunchTime" -> minTaskLaunchTime,
      "maxTaskFinishTime" -> maxTaskFinishTime,
      "parentStageIDs" -> parentStageIDs.mkString("[", ",", "]"),
      "taskExecutionTimes" -> taskExecutionTimes.mkString("[", ",", "]"),
      "taskPeakMemoryUsage" -> taskPeakMemoryUsage.mkString("[", ",", "]")
    ) ++ super.getStartEndTime()).asJava
  }
}

object StageTimeSpan {

  private val gson = new Gson()

  def getTimeSpan(json: JsonObject): mutable.HashMap[Int, StageTimeSpan] = {
    val map = new mutable.HashMap[Int, StageTimeSpan]
    import scala.collection.JavaConverters._
    for (elem <- json.entrySet().asScala) {
      val value = elem.getValue.getAsJsonObject
      val timeSpan = new StageTimeSpan(
        value.get("stageID").getAsInt,
        value.get("numberOfTasks").getAsLong
      )
      timeSpan.stageMetrics = AggregateMetrics.getAggregateMetrics(value.get("stageMetrics")
        .getAsJsonObject)
      timeSpan.minTaskLaunchTime = value.get("minTaskLaunchTime").getAsLong
      timeSpan.maxTaskFinishTime = value.get("maxTaskFinishTime").getAsLong

      timeSpan.parentStageIDs = gson.fromJson(value.get("parentStageIDs").getAsString,
        classOf[java.util.List[Double]]).asScala.map(_.toInt)
      timeSpan.taskExecutionTimes = gson.fromJson(value.get("taskExecutionTimes").getAsString,
        classOf[java.util.List[Double]]).asScala.map(_.toInt).toArray
      timeSpan.taskPeakMemoryUsage = gson.fromJson(value.get("taskPeakMemoryUsage").getAsString,
        classOf[java.util.List[Double]]).asScala.map(_.toLong).toArray

      timeSpan.addStartEnd(value)

      map.put(elem.getKey.toInt, timeSpan)
    }
    map
  }

}
