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
package com.qubole.sparklens.common

import com.google.gson.{Gson, JsonObject}

case class ApplicationInfo (var applicationID:String = "NA",
                            var startTime:Long = 0L,
                            var endTime:Long = 0L) {
  override def toString(): String = {
    import scala.collection.JavaConverters._

    Map("applicationID" -> applicationID, "startTime" -> startTime, "endTime" -> endTime).asJava
      .toString
  }
}

object ApplicationInfo {

  def getObject(appInfo: JsonObject): ApplicationInfo = {
    ApplicationInfo(
      appInfo.get("applicationID").getAsString,
      appInfo.get("startTime").getAsLong,
      appInfo.get("endTime").getAsLong)
  }
}