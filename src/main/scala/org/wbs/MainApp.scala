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
  val userBBDD = "root"
  val passBBDD = "1"
  val pathBBDD = "jdbc:mysql://localhost:3306/clients"
  val tableBBDD = "crawler_data"

  //Inicializar Spark
  val sparkConf = new SparkConf().setAppName("Phrases").setMaster("local[4]")
  //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    //opcionBG puede ser ID para BG o String(path o text)
    val opBG = menuBG()
    val optionBG : Either[String,Int] = try{
      Right(opBG.toInt)
    }catch {
      case e: Exception => Left(opBG)
    }
    //opcionFG puede ser ID o path
    val opFG = menuFG()
    val optionFG : Either[String,Int] = try{
      Right(opFG.toInt)
    }catch {
      case e: Exception => Left(opFG)
    }

//    //Conexion BBDD
//    val properties = new Properties()
//    properties.setProperty("user", userBBDD)
//    properties.setProperty("password", passBBDD)
//    properties.setProperty("driver", "com.mysql.jdbc.Driver")
//
//    //Lectura BBDD completa
//    val jdbcDF = sqlContext.read
//      .jdbc(pathBBDD,tableBBDD,properties)

//    Selecciona solo texto y para cada texto realiza el analisis
//    val justText = jdbcDF.select("text","id", "keyword").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id"), row.getAs[String]("keyword")))

//    var processedText:(RDD[(String, Double, Double, Double)], Tuple2[String,Int]) = _
    //Selecciona BG text por ID o lectura desde path
    optionBG match {
      case Right(id) => {
        val df = getBBDD()
        val justText = df.select("text","id", "keyword").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id"), row.getAs[String]("keyword")))
        ProcessText.createBG(justText.map(x=>(x._1,x._2)).filter(x => x._2 == id))
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
    //Selecciona FG text por ID o lectura desde path
    optionFG match {
      case Right(id) => {
        val df = getBBDD()
        val justText = df.select("text","id", "keyword").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id"), row.getAs[String]("keyword")))
        val processedText = justText.map(x=>(x._1,x._2)).map(x =>{
            ProcessText.setFG(x)
            ProcessText.main()
          }
        )
        val result = processedText.map(x => AnalyzeResult.analyze(x))
        result.take(result.count().toInt).foreach(println)
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

//    //Selecciono BG from BBDD
//    ProcessText.createBG(justText.filter(x => x._2 == 33000))

//    //Proceso el texto
//    val processedText = justText.map(x=>(x._1,x._2)).map(x => ProcessText.main(x))


//    //Selecciono solo un texto para las pruebas!
//    val textId = justText.filter(x=> x._2==33333)
//    //Proceso el texto de pruebas
//    val processedText = textId.map(x => ProcessText.main(x))

//    println(processedText.count())
//    processedText.foreach(x => x._1.take(x._1.count().toInt).foreach(println))

//    val result = processedText.map(x => AnalyzeResult.analyze(x))
//    val result = ProcessText.main(justText.take(1)(0))
//    println("resultkausdhfadksuhfdasfb;asdki;kjvfbdaslvgfbdas/gfkjhdas;fkjbad.sfkjhdas;nf")
//    val test = sc.broadcast(result.map(x=>x.collect()))
//    val test = result.flatMap(_.collect().toSeq).keyBy(x=>x)
//    val test2 = test.map(x=> x)
//    result.take(20).foreach(println)
//    val res =result.count()
//    result.take(result.count().toInt).foreach(println)

//    println("Datos antes de procesar: " + justText.count())
//    println("Datos dspues de procesar: " + processedText.count())
//    println("Resultados: " + res)
    sc.stop()
  }
  def menuBG(): String = {
    println("************ MENU *************")
    println("* 1. Select BG text from BBDD *")
    println("* 2. Select BG text from PATH *")
    println("* 3. Input text for BG        *")
    println("*******************************")
    val input = readLine()
    input match {
      case "1" => {
        println("* Set ID for BG text *")
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
        println("Set input text")
        var text = readLine()
        while(readLine().length() > 0){
          text += readLine()
        }
        println(text)
        ""
      }
      case _ => {
        println("Please select a numeric option")
        println()
        menuBG()
      }
    }
  }
  def menuFG(): String = {
    println("*******************************")
    println("* 1. Select FG text from BBDD *")
    println("* 2. Select FG text from PATH *")
    println("* 3. Input text for FG        *")
    println("*******************************")
    val input = readLine()
    input match {
      case "1" => {
        println("* Selecting BBDD for FG text *")
        val id = "1"
        id
      }
      case "2" => {
        println("* Set path for FG text *")
        var path = readLine()
        path = "/" + path
        path
      }
      case "3" => {
        println("Set input text")
        val text = readLine()
        text
      }
      case _ => {
        println("Please select a numeric option")
        println()
        menuFG()
      }
    }
  }

  def getBBDD(): DataFrame = {
    //Conexion BBDD
    val properties = new Properties ()
    properties.setProperty ("user", userBBDD)
    properties.setProperty ("password", passBBDD)
    properties.setProperty ("driver", "com.mysql.jdbc.Driver")
    //Lectura BBDD completa
    val jdbcDF = sqlContext.read
    .jdbc (pathBBDD, tableBBDD, properties)
    jdbcDF
  }
}
