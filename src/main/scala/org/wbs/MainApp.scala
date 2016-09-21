package org.wbs

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by cristu on 15/09/16.
  */
object MainApp {

  //Inicializar Spark
  val sparkConf = new SparkConf().setAppName("Phrases").setMaster("local[4]")
  //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    //Conexion BBDD
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "1")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    //Lectura BBDD completa
    val jdbcDF = sqlContext.read
      .jdbc("jdbc:mysql://localhost:3306/clients","crawler_data",properties)
    //Selecciona solo texto y para cada texto realiza el analisis
    val justText = jdbcDF.select("text","id").map(row=>(row.getAs[String]("text"),row.getAs[Int]("id")))

//    //Proceso el texto
    val processedText = justText.map(x => ProcessText.main(x))


//    //Selecciono solo un texto para las pruebas!
//    val textId = justText.filter(x=> x._2==33333)
//    //Proceso el texto de pruebas
//    val processedText = textId.map(x => ProcessText.main(x))

    println(processedText.count())
//    processedText.foreach(x => x._1.take(x._1.count().toInt).foreach(println))

    val result = processedText.map(x => AnalyzeResult.analyze(x))
//    val result = ProcessText.main(justText.take(1)(0))
//    println("resultkausdhfadksuhfdasfb;asdki;kjvfbdaslvgfbdas/gfkjhdas;fkjbad.sfkjhdas;nf")
//    val test = sc.broadcast(result.map(x=>x.collect()))
//    val test = result.flatMap(_.collect().toSeq).keyBy(x=>x)
//    val test2 = test.map(x=> x)
//    result.take(20).foreach(println)
//    val res =result.count()
    result.take(result.count().toInt).foreach(println)

//    println("Datos antes de procesar: " + justText.count())
//    println("Datos dspues de procesar: " + processedText.count())
//    println("Resultados: " + res)
    sc.stop()
  }
}
