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

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import scalaj.http.{HttpOptions, Http}


object TwitterKeywordExtractor {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterKeywordExtractor <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[8]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    hashTags.foreachRDD {
      rdd => {
        val top1000 = rdd.take(1000)
        var max = 0

        for (i <- 0 until top1000.size) {

          val word = top1000(i).replace("#", "")

          println("Data transfer - word: " + word + ", num: " + "0")

          val cal: Calendar = Calendar.getInstance()
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSZ")
          val epoch = cal.getTimeInMillis

          val data = """{"@timestamp":"""" + sdf.format(cal.getTime) + """","word":"""" + word + """"}"""

          val result = Http.postData("http://bigtb04:9200/first/twitter/", data)
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(10000))
            .responseCode

          //println("ResponseCode From Server: " + result)

        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}