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

package org.apache.spark.example

import org.apache.spark.{Logging, SparkContext, SparkConf}

/**
 * This is a example of Apache Spark application.
 */
object SampleApplication {

  /**
   * This method is called as a Apache Spark application.
   * This application only counts an input file.
   *
   * @param args an Array of String
   */
  def main(args: Array[String]) {
    //log.info("This Applicatino starts.")

    // Sets the input arguments.
    val master = args(0)
    val maxCores = args(1)
    val inputFile = args(2)

    // Makes a Spark context.
    val appName = "Sample Spark Application"
    val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(master)
        .set("spark.cores.max", maxCores)
    val sc = new SparkContext(conf)

    // Executes this Spark application.
    val rdd = sc.textFile(inputFile)
    val lines = rdd.count()

    // Prints the result.
    println(s"# Lines: ${lines}")

    // Tears down.
    sc.stop()
   // log.info("This Applicatino ends.")
  }
}