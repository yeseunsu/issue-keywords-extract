import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import java.text.SimpleDateFormat
import java.util.Calendar
import scalaj.http.{HttpOptions, Http}

/**
 * Created by silverw on 2014-06-18.
 */
object SparkWordProcess {

  // Some configurations
  val spark_server: String = "bigtb05"
  val app_name: String = "WordExtractor"
  val spark_home: String = "/home/fire/spark/"
  val spark_executor_memory: String = "8g"
  val jar_location: String = "/home/fire/keyword-streaming/target/scala-2.10/topwords_2.10-1.0.jar"
  val listen_host = "bigtb04"
  //val listen_host = "localhost"
  val listen_port = 9998

  val dropwords = Array[String]("ㄷ", "ㄷㄷ", "ㄷㄷㄷ", "ㄷㄷㄷㄷ", "有", "無", "ㅠㅠ", "ㅠ", "jpg", "?", "??", "ㅋ", "ㅋㅋ", "ㅋㅋㅋ", "ㅋㅋㅋㅋ", "ㅋㅋㅋㅋㅋ", "..", ".")
  val dropwords2 = Array[String]("vs", "오늘", "ㅎ", "좀", "있나요", "이거", "왜", "분", "방금", "현직", "진짜", "요즘", "지금", "많이", "유", "정말", "질문", "어떻게", "이", "근데", "계신가요", "어떤가요", "흔한", " 하", "아시는분", "또", "있", "아", "역시", "현재", "헐", "했나요", "없나요", "참", "아닌가요", "다", "뭔가요", "제일", "뭐가", "저", "때", "계속", "너무", "gt", "하면", "1", "2", "3", "얼마나", "되나요", "제", "ㅊㅈ", "내일", "가나요", "어제", "이런", "다시", "저도", "제가", "저도", "혹시", "같이", "전", "잘", "있을까요", "이게", "우", "안", "부탁드립니다", ">좋아하는", "중", "있나유", "좋겠네요", "주세여", "추천", "자게", "그냥", "더", "와", "뭐", "이유가", "제가에", "해주세요", "있네요", "ㅠ", "하고", "무슨", "가장", "하나요", "자게에", "자게이", "수", "추천좀", "관련", "추천할만한", "싶은", "것", "이유", "좋은", "분들", "있습니다", "그", "보", "엄청", "보고", "없", "어떤", "ㅋㅋㅋㅋㅋㅋㅋㅋ", "이번", "보면", "한", "이제", "위엄", "여자", "사람", "추천해주세요", "않나요", "없네요", "할", "있죠", "이라고", "후에", "좋나요", "입니다", "아시는", "처음", "나온", "좋을까요", "어디서", "ㅊㅈ", "ㅊㅈ들", "같은", "쓰시나요", "인가요", "이렇게", "있는", "하네요", "하는분", "해도", "하는", "하네요", "있는", "먼저" , "어때요", "듯", "그래도" ,"0", "유", "할듯")


  var broadcastVar: Broadcast[Int] = null
  var broadcastVar2: Broadcast[Int] = null


  def updateFunction(values: Seq[Int], state: Option[Int]): Option[Int] = {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]) {

    //val conf = new SparkConf().setMaster("spark://" + spark_server + ":7077").setAppName(app_name).setSparkHome(spark_home).set("spark.executor.memory", spark_executor_memory).setJars(Seq(jar_location)).set("spark.cleaner.ttl", "10000")
    val conf = new SparkConf().setMaster("local[8]").setAppName(app_name).set("spark.cleaner.ttl", "10000")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val text = ssc.socketTextStream(listen_host, listen_port)

    doSlr(text, sc)
    doMlbPark(text, sc)

    ssc.start()
    ssc.awaitTermination()
  }


  def doMlbPark(text: DStream[String], sc: SparkContext) {

    if (broadcastVar2 == null)
      broadcastVar2 = sc.broadcast(0)

    val slrw2 = text.filter(l => l.contains("mlbpark:::")).map{ l =>
      val splits = l.split(":::")
      val num = splits(1)
      val words = splits(2).replaceAll("<strong>","").replaceAll("</strong>", "")
      //println((num, words.split(" ")))
      (num, words.split(" "))
    }


    val slrw3 = slrw2.flatMap { l =>
      var newarr = Array[String]()
      for (i <- 0 until l._2.size) {

        var word = l._2(i)

        dropwords.foreach { wor =>
          word = word.replace(wor, "")
        }
        newarr = newarr :+ word + 30.toChar + l._1
      }
      newarr
    }


    val slrw4 = slrw3.filter(l => l.split(30.toChar)(1).toInt > broadcastVar2.value)
    //println(slrw4.print())

    var dup = Array[(Int, String)]()
    slrw4.foreachRDD {
      rdd => {
        val top1000 = rdd.take(1000)
        var max = 0

        for (i <- 0 until top1000.size) {
          val split = top1000(i).split(30.toChar)
          val word = split(0)
          val num = split(1)

          if (num.toInt > max)
            max = num.toInt

          if (word != "" && word != "," && word != "." && !dropwords2.contains(word) && !dup.contains((num.toInt, word))) {

            dup = dup :+(num.toInt, word)
            println("Data transfer - word: " + word + ", num: " + num)

            val cal: Calendar = Calendar.getInstance()
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSZ")
            val epoch = cal.getTimeInMillis

            val data = """{"@timestamp":"""" + sdf.format(cal.getTime) + """","word":"""" + word + """","number":"""" + num + """"}"""

            val result = Http.postData("http://bigtb04:9200/first/slrwords4/", data)
              .header("Content-Type", "application/json")
              .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(10000))
              .responseCode

            //println("ResponseCode From Server: " + result)

          }
        }

        if (max > broadcastVar2.value) {
          println("-- Setting Max Number: " + max)
          broadcastVar2 = sc.broadcast(max)
        }
      }
    }

  }

  def doSlr(text: DStream[String], sc: SparkContext) {

    if (broadcastVar == null)
      broadcastVar = sc.broadcast(0)

    val slrw = text.filter(l => l.contains("slrclub:::")).map(l => l.drop(10))
    val slrw2 = slrw.map(line => (line.split(">")(1).split(";")(1).drop(3).dropRight(1).toInt, line.split(">")(2).dropRight(3).split(" ")))
    val slrw3 = slrw2.flatMap { l =>
      var newarr = Array[String]()
      for (i <- 0 until l._2.size) {

        var word = l._2(i)

        dropwords.foreach { wor =>
          word = word.replace(wor, "")
        }
        newarr = newarr :+ word + 30.toChar + l._1
      }
      newarr
    }


    val slrw4 = slrw3.filter(l => l.split(30.toChar)(1).toInt > broadcastVar.value)
    //println(slrw4.print())

    var dup = Array[(Int, String)]()
    slrw4.foreachRDD {
      rdd => {
        val top1000 = rdd.take(1000)
        var max = 0

        for (i <- 0 until top1000.size) {
          val split = top1000(i).split(30.toChar)
          val word = split(0)
          val num = split(1)

          if (num.toInt > max)
            max = num.toInt

          if (word != "" && word != "," && word != "." && !dropwords2.contains(word) && !dup.contains((num.toInt, word))) {

            dup = dup :+(num.toInt, word)
            println("Data transfer - word: " + word + ", num: " + num)

            val cal: Calendar = Calendar.getInstance()
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSZ")
            val epoch = cal.getTimeInMillis

            val data = """{"@timestamp":"""" + sdf.format(cal.getTime) + """","word":"""" + word + """","number":"""" + num + """"}"""

            val result = Http.postData("http://bigtb04:9200/first/slrwords4/", data)
              .header("Content-Type", "application/json")
              .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(10000))
              .responseCode

            //println("ResponseCode From Server: " + result)

          }
        }

        if (max > broadcastVar.value) {
          println("-- Setting Max Number: " + max)
          broadcastVar = sc.broadcast(max)
        }


      }
    }

  }

}
