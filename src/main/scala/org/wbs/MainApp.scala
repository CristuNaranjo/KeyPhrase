package org.wbs

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by cristu on 15/09/16.
  */
object MainApp {

  //Datos para la conexion BBDD
  val userDB = "root"
  val passDB = "1"
  val pathDB = "jdbc:mysql://localhost:3306/clients"
  val tableDB = "crawler_data"

  //Inicializar Spark
  val sparkConf = new SparkConf().setAppName("Phrases").setMaster("local[4]")
  //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    //opcionBG puede ser ID para BG o String(path o text)
    val opBG = menuBG()
    val optionBG : Either[String,Int] = try{
      if(opBG.toInt > 2) {
        println("Wrong option, closing...")
        sys.exit()
      }
      Right(opBG.toInt)
    }catch {
      case e: Exception => Left(opBG)
    }
    //Selecciona BG text por ID o lectura desde path
    optionBG match {
      case Right(id) => {
        if(id == 1) {
          val df = getDB()
          val justText = df.select("text","id", "keyword").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id"), row.getAs[String]("keyword")))
          ProcessText.createBG(justText.map(x=>(x._1,x._2)).filter(x => x._2 == id))
        }else if(id == 2){
          println("********** Creating BG from DB **********")
          val df = getDB()
          val DBText = df.select("text").map(row=>(row.getAs[String]("text")))
          val numKeyWords = 20
          ProcessText.getBGfromDB(DBText, numKeyWords)

        }
      }
      case Left(str) => {
        if(str.startsWith("/")){
          val path = str.replaceFirst("/","")
          ProcessText.createBGPath(path)
        }
        else{
          ProcessText.createBGText(str)
        }
      }
    }

    //opcionFG puede ser ID o path
    val opFG = menuFG()
    val optionFG : Either[String,Int] = try{
      Right(opFG.toInt)
    }catch {
      case e: Exception => Left(opFG)
    }

    //Selecciona FG text por ID o lectura desde path
    optionFG match {
      case Right(id) => {
        val df = getDB()
        val justText = df.select("text","id", "keyword").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id"), row.getAs[String]("keyword")))
        val processedText = justText.map(x=>(x._1,x._2)).map(x =>{
            ProcessText.setFG(x)
            ProcessText.main()
          }
        )
        val result = processedText.map(x => AnalyzeResult.analyze(x))
        val resultOrdered = result.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))
        resultOrdered.take(resultOrdered.count().toInt).foreach(println)
      }
      case Left(str) => {
        if(str.startsWith("/")) {
          val path = str.replaceFirst("/", "")
          ProcessText.createFG(path)
          val processedText = ProcessText.main()
          val result = AnalyzeResult.analyze(processedText)
          println(result)
        }else{
          ProcessText.createFGText(str)
          val processedText = ProcessText.main()
          val result = AnalyzeResult.analyze(processedText)
          println(result)
        }
      }
    }

    sc.stop()
  }
  def menuBG(): String = {
    println("************ MENU BG **************")
    println("**                               **")
    println("**  1. Select BG text from DB    **")
    println("**  2. Select BG text from PATH  **")
    println("**  3. Input text for BG         **")
    println("**                               **")
    println("***********************************")
    val input = readLine()
    input match {
      case "1" => {
        println("* 1. Select ID for BG text *")
        println("* 2. Get BG from all DB *")
        val id = readLine()
        id
      }
      case "2" => {
        println("* Set path for BG text *")
        var path = readLine()
        path = "/" + path
        path
      }
      case "3" => {
        println("* Set input text *")
        var output = ""
        def read():Unit={
          val input = readLine()
          if(input.isEmpty) ()
          else {
            output += input
            read
          }
        }
        read
        output
      }
      case _ => {
        println("Please select a numeric option")
        println()
        menuBG()
      }
    }
  }
  def menuFG(): String = {
    println()
    println("************ MENU FG **************")
    println("**                               **")
    println("**  1. Select FG text from DB    **")
    println("**  2. Select FG text from PATH  **")
    println("**  3. Input text for FG         **")
    println("**                               **")
    println("***********************************")
    val input = readLine()
    input match {
      case "1" => {
        println("* Selecting BBDD for FG text *")
        val id = "1"
        println("********** Loading **********")
        id

      }
      case "2" => {
        println("* Set path for FG text *")
        var path = readLine()
        path = "/" + path
        println("********** Loading **********")
        path

      }
      case "3" => {
        println("* Set input text *")
        var output = ""
        def read():Unit={
          val input = readLine()
          if(input.isEmpty) ()
          else {
            output += input
            read
          }
        }
        read
        println("********** Loading **********")
        output
      }
      case _ => {
        println("Please select a numeric option")
        println()
        menuFG()
      }
    }
  }

  def getDB(): DataFrame = {
    //Conexion BBDD
    val properties = new Properties ()
    properties.setProperty ("user", userDB)
    properties.setProperty ("password", passDB)
    properties.setProperty ("driver", "com.mysql.jdbc.Driver")
    //Lectura BBDD completa
    val jdbcDF = sqlContext.read
    .jdbc (pathDB, tableDB, properties)
    jdbcDF
  }
}
