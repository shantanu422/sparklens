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
package com.qubole.sparklens.analyzer

import java.io.StringWriter
import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens._
import com.qubole.sparklens.helper.HDFSConfigHelper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * Interface for creating new Analyzers
 */

trait AppAnalyzer {
  def analyze(ac: AppContext): Any = {
    analyze(ac, ac.appInfo.startTime, ac.appInfo.endTime)
  }

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): Any

  import java.text.SimpleDateFormat
  val DF = new SimpleDateFormat("hh:mm:ss:SSS")
  val MINUTES_DF = new SimpleDateFormat("hh:mm")

  /*
  print time
   */
  def pt(x: Long) : String = {
    DF.format(new  Date(x))
  }
  /*
  print duration
   */
  def pd(millis: Long) : String = {
    "%02dm %02ds".format(
      TimeUnit.MILLISECONDS.toMinutes(millis),
      TimeUnit.MILLISECONDS.toSeconds(millis) -
        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
    )
  }

  def pcm(millis: Long) : String = {
    val millisForMinutes = millis % (60*60*1000)

    "%02dh %02dm".format(
      TimeUnit.MILLISECONDS.toHours(millis),
      TimeUnit.MILLISECONDS.toMinutes(millisForMinutes))
  }

  implicit class PrintlnStringBuilder(sb: StringBuilder) {
    def println(x: Any): StringBuilder = {
      sb.append(x).append("\n")
    }
    def print(x: Any): StringBuilder = {
      sb.append(x)
    }
  }
}

object AppAnalyzer {
  def startAnalyzers(appContext: AppContext, sparkConf: SparkConf): Unit = {

    val metrics = AppMetrics(sparkConf.getAll.map(x=> Property(x._1, x._2)).toList,
      new AppTimelineAnalyzer().analyze(appContext).asInstanceOf[AppTimeline],
      new EfficiencyStatisticsAnalyzer().analyze(appContext).asInstanceOf[EfficiencyStatistics],
      new ExecutorTimelineAnalyzer().analyze(appContext).asInstanceOf[ExecutorTimeline],
      new ExecutorWallclockAnalyzer().analyze(appContext).asInstanceOf[ExecutorEstimates],
      new HostTimelineAnalyzer().analyze(appContext).asInstanceOf[HostInfo],
      new SimpleAppAnalyzer().analyze(appContext).asInstanceOf[Map[String, Map[String, Any]]],
      new StageSkewAnalyzer().analyze(appContext).asInstanceOf[List[StageSkewMetrics]])


//    list.foreach( x => {
//      try {
//        val output = x.analyze(appContext)
//        val dumpDir = getDumpDirectory(sparkConf) + "/" + sparkConf.get("spark.app.id", "testid")
//        println(s"Saving sparkLens data to ${dumpDir}")
//        val fs = FileSystem.get(new URI(dumpDir), HDFSConfigHelper.getHadoopConf(Some(sparkConf)))
//        val stream = fs.create(new Path(s"${dumpDir}/reports_${x.getClass.getSimpleName}.sparklens.txt"))
//        val jsonString = output
//        stream.writeBytes(jsonString)
//        stream.close()
//        println(output)
//      } catch {
//        case e:Throwable => {
//          println(s"Failed in Analyzer ${x.getClass.getSimpleName}")
//          e.printStackTrace()
//        }
//      }
//    })

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, metrics)
    val json = out.toString()

    val dumpDir = getReportingDirectory(sparkConf) + "/appId=" + sparkConf.get("spark.app.id", "testid")
    println(s"Saving sparkLens data to ${dumpDir}")
    val fs = FileSystem.get(new URI(dumpDir), HDFSConfigHelper.getHadoopConf(Some(sparkConf)))
    val stream = fs.create(new Path(s"${dumpDir}/reports_custom_sparklens.json"))
    stream.writeBytes(json)
    stream.close()
    println(json)

  }

}

case class AppMetrics(sparkConfigs: List[Property], appTimeline: AppTimeline, efficiencyStatistics: EfficiencyStatistics,
                      executorTimeline: ExecutorTimeline, executorEstimates: ExecutorEstimates,
                      hostInfo: HostInfo, simpleMetrics: Map[String, Map[String, Any]],
                      stageSkewMetrics: List[StageSkewMetrics])

case class Property(key: String, value: String)
