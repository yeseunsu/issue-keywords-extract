
import java.net.{ServerSocket, Socket}
import java.io._
import java.nio.charset.Charset
import scalaj.http.{HttpOptions, Http}

/**
 * Created by Eunsu Yun on 2014-06-18.
 */
class WordExtractTransfer (socket_host:String, port:Int) {

  var socket_server:ServerSocket = null
  var socket:Socket = null
  var os:OutputStream = null
  var bw:BufferedWriter =null

  def socketOpen(){

    socket_server = new ServerSocket(port)
    socket = socket_server.accept()
    os = socket.getOutputStream
    bw = new BufferedWriter(new OutputStreamWriter(os))

    println("Socket stream initialized.. (connected)")

  }

  def init(){

    socketOpen()

  }

  def parseSlrclub(){

    var result:Http.Request = null
    try{
      result = Http.get("http://www.slrclub.com/bbs/zboard.php?id=free").option(HttpOptions.readTimeout(10000))
      val code = result.responseCode

      if (code >= 200){
        //println(result.asString)
        val str = result.asString.split("\n")

        str.foreach { line =>
          if (line.contains("td class=\"sbj\"") && !line.contains("[공지]")){
            println(line)
            bw.write("slrclub:::" + line + System.getProperty("line.separator"))

          }
        }
      }
    } catch {
      case e:Exception => {
        println(e.getMessage)
      }
    }
  }


  def parseMlbPark(){

    var result:Http.Request = null
    try{
      result = Http.get("http://mlbpark.donga.com/mbs/articleL.php?mbsC=bullpen2").option(HttpOptions.readTimeout(10000))
      val code = result.responseCode

      if (code >= 200){
        //println(result.asString)
        val str = result.charset("euc-kr").asString.split("\n")

        var linenum = 0
        str.foreach { line =>
          if (line.contains("bullpen2&mbsIdx")){
            linenum = line.split("&")(1).replace("mbsIdx=", "").toInt

          } else{
            if (linenum != 0 && line.contains("<strong>") && !line.contains("a href")){
              val newLine = linenum + ":::" + line.trim
              println(newLine)
              bw.write("mlbpark:::" + linenum + "::: " + line + System.getProperty("line.separator"))
              linenum = 0
            }
          }
        }
      }

    } catch {
      case e:Exception => {
        println(e.getMessage)
      }
    }

  }


  def loop(){

    do{
      parseSlrclub
      Thread.sleep(1000)
      parseMlbPark
      Thread.sleep(20000)

    }while(true);
  }


  def close(){
    bw.flush
    bw.close
    os.close
    socket.close
    socket_server.close
  }
}

object WordExtractTransferRun{

  def main(args: Array[String]) {

    val socket_host = "bigtb04"
    //val socket_host = "localhost"
    val port = 9998

    val cci:WordExtractTransfer = new WordExtractTransfer(socket_host, port)

    cci.init()
    cci.loop()
    cci.close()

  }
}