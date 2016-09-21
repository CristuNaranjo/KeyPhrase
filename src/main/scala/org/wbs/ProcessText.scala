package org.wbs

import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.io.Source

/**
  * Created by cristu on 15/09/16.
  */
object ProcessText {

  val sqlContext = MainApp.sqlContext
  val sc = MainApp.sc
  //Cargar StopWords
  val stopWordsGer = Source.fromFile("resources/stopWordsGer.txt").getLines().mkString(" ")


  def main(text: Tuple2[String,Int]): (RDD[(String, Double, Double, Double)], Tuple2[String,Int]) = {
//Tuple2[Tuple2[String,Int],RDD[String]]

    //Pesos para realizar el analisis, probar a realizar una combinacion EXPONENCIAL de los modelos
    val phraseWeight = 10 //peso phraseness para la puntuacion final
    val infoWeight = 100 // peso informativeness para la puntuacion final

    //Separo en palabras
    val tokenText =sc.parallelize(Seq(tokenize(text._1)))

    //Creo el dataframe para pasarle a los N-Gram
    val dataFrameText = sqlContext.createDataFrame(tokenText.map(Tuple1.apply)).toDF("text")

    //Define N-Grams
    val ngram1 = new NGram().setInputCol("text").setOutputCol("ngrams").setN(1)
    val ngram2 = new NGram().setInputCol("text").setOutputCol("ngrams").setN(2)

    //Creo los n-gram para el texto BG ()
    val BGGram = createNGram(getBG(sc, sqlContext), ngram1, ngram2, stopWordsGer)
    val BGram1 = BGGram._1
    val BGram2 = BGGram._2
    BGram1.persist()
    BGram2.persist()

    //Creo los n-gram para el texto de la BBDD
    val FGGram = createNGram(dataFrameText, ngram1, ngram2, stopWordsGer)
    val FGram1 = FGGram._1
    val FGram2 = FGGram._2

    //Convierto los bigram a un string para luego filtrar facilmente (string.contains)
    val BigramsFG = FGram2.map(x=>x._1).collect().mkString(" ")

    //Proceso los bigrams para obtener (palabra(0), (palabra(1), FGcuenta, BGcuenta)) con palabra(0) del bigram como clave
    val processedBigrams = processBigrams(FGram2, BGram2)

    //Cuento los Bigrams totales
    val uniqueBigramsCount = processedBigrams.count()

    val (totalBigramsFG, totalBigramsBG)  = processedBigrams
      .map(x => (x._2._2._1, x._2._2._2.toLong)) //Me quedo solo con las cuentas, sin palabras
      .reduce((x,y) => (x._1+y._1, x._2+y._2)) //Reduzco y hago la suma para calcular el total

    //Repito el proceso para los unigrams
    val processedUnigrams = processUnigrams(FGram1,BGram2)
    val uniqueUnigramsCount = processedUnigrams.count()
    val (totalUnigramsFG, _) = processedUnigrams.map(x => (x._2._1.toLong, x._2._2.toLong))
          .reduce((x,y) => (x._1+y._1, x._2+y._2))


    //Agrupo los unigram con la primera palabra de los bigram (luego lo hago con la segunda)
    val groupedByFirstWord = processedUnigrams.cogroup(processedBigrams)


    //Ahora puedo anadir a la primera palabra del bigram la cuenta del unigram
    val bigramsWithFirstWordCount = groupedByFirstWord
      .flatMap{ x =>
        val (firstWord, (unigramIter, bigramIter)) = x
        var unigramFG = 0
        try{
          val (unigramFGtmp, _) = unigramIter.head
          unigramFG = unigramFGtmp
        }catch{
          case e: Exception => {
            println("EEEeeeeeeerrrrooooooooorrr: "+e)
          }
        }
        finally {
        }
        bigramIter.map{ bigram =>
          val (secondWord, (bigramFG, bigramBG)) = bigram
          //Lo dejo preparado para agrupar con la segunda palabra del bigram
          (secondWord, (firstWord, bigramFG, bigramBG, unigramFG))
        }
      }

   //Agrupo con la segunda palabra del bigram
   //Ahora puedo anadir a la segunda palabra del bigram la cuenta del unigram
    // Obtengo un dataframe con la primera palabra, segunda palabra, la cuenta de cuantas
    // veces aparece el bigram en FG, cuenta de bigram en BG, cuenta de unigram(palabra1)
    // en FG y cuenta de unigram(palabra2) en FG
    val groupedBySecondWord = processedUnigrams.cogroup(bigramsWithFirstWordCount)

    //RDD[(String, String, Int, Int, Int, Int)]
    val bigramsWithUnigramData = groupedBySecondWord
      .flatMap{ x =>
        val (secondWord, (unigramIter, bigramIter)) = x
        var unigramFG = 0
        try{
          val (unigramFGtmp,_) = unigramIter.head
          unigramFG = unigramFGtmp
        }catch{
          case e: Exception => {
            println("EEEeeeeeeerrrrooooooooorrr: "+e)
          }
        }
        finally{
        }
        bigramIter.map{ bigram =>
          val (firstWord, bigramFG, bigramBG, firstFG) = bigram
          (firstWord, secondWord, bigramFG, bigramBG, firstFG, unigramFG)
        }
    }

    //scoresOfBigrams indica la puntuacion final para cada bigram utilizando la diferencia de probabilidades KL-Divergence
    val scoresOfBigrams = bigramsWithUnigramData.map{ x =>
      def klDivergence(p: Double, q: Double) = {
        p * (Math.log(p) - Math.log(q))
      }
      val (firstWord, secondWord, bigramFG, bigramBG, firstFG, secondFG) = x
//      println(x)
      //calculo probabilidades
      val pBigramFG = (bigramFG + 1).toDouble / (uniqueBigramsCount + totalBigramsFG).toDouble
      val pBigramBG = (bigramBG + 1).toDouble / (uniqueBigramsCount + totalBigramsBG).toDouble
      val pFirstFG  = (firstFG  + 1).toDouble / (uniqueUnigramsCount + totalUnigramsFG).toDouble
      val pSecondFG = (secondFG + 1).toDouble / (uniqueUnigramsCount + totalUnigramsFG).toDouble

      val phraseness = klDivergence(pBigramFG, pFirstFG * pSecondFG)
      val informativeness = klDivergence(pBigramFG, pBigramBG)
      (firstWord + " " + secondWord, phraseWeight * phraseness + infoWeight * informativeness, phraseness, informativeness)

    }

//    println(scoresOfBigrams.count())
    scoresOfBigrams.cache()
//    println(scoresOfBigrams)
    (scoresOfBigrams, text)
  }
  private def tokenize(text: String): Seq[String] = {
    // Paso a minusculas, elimino signos de puntuacion y espacios en blanco
    text.toLowerCase.replaceAll("\\p{P}", "").replaceAll("\\s+"," ").split("\\s").toSeq
  }
  private def createNGram(documentDF: DataFrame, n1:NGram, n2:NGram, stopWords:String): (RDD[(String, Int)], RDD[Tuple2[String,Int]]) ={
    //    RDD[(String, Int)]
    //Transformo el texto en n-gram 1 y 2
    val df1 = n1.transform(documentDF)
    val df2 = n2.transform(documentDF)
    //    df.show(20)
    val resultDF1 = df1.map(frame => frame.getAs[Stream[String]]("ngrams").toList)
      .flatMap(x=>x) //separo arrays en palabras
      .filter(x => (x!="" || x!=" ")) // elimino los espacios en blanco {n1}
      .filter(word => !stopWords.contains(word)) //elimino las stopWords
      .map(ngram => (ngram,1)) //convierto en tuple (palabra,numero)
      .reduceByKey((a,b) => a + b) //cuento las palabras iguales
      .map(tuple => (tuple._2, tuple._1))// cambio el orden para hacer key:value y poder ordenar por numero, no por palabra
      .sortByKey(false) //ordena descendente
      .map(tuple => (tuple._2, tuple._1))//vuelvo a hacer el cambio para luego agrupar por palabras
    //    result.take(20).foreach(println)

    val resultDF2 = df2.map(frame => frame.getAs[Stream[String]]("ngrams").toList)
      .flatMap(x=>x) //separo arrays en palabras
      .filter(bigram => {
        val words = bigram.split(" ")
        if(words.length>1){
          !(stopWords.contains(words(0)) || stopWords.contains(words(1)))
        }else if(words.length == 1){
          !stopWords.contains(words(0))
        }else{
          false
        }
       })
      .map(ngram => (ngram,1)) //convierto en tuple (palabra,numero)
      .reduceByKey((a,b) => a + b) //cuento las palabras iguales
      .map(tuple => (tuple._2, tuple._1))// cambio el orden para hacer key:value y poder ordenar por numero, no por palabra
      .sortByKey(false) //ordena descendente
      .map(tuple =>{(tuple._2,tuple._1)})//vuelvo a hacer el cambio y dejo
    //      .map(tuple => tuple

    (resultDF1,resultDF2)
  }
  private def getBG(sc:SparkContext,sqLContext: SQLContext): DataFrame ={
    var textPath = "resources/Orthopadie.txt"
    val one = sc.textFile(textPath).map(line => tokenize(line))
    val two = sqLContext.createDataFrame(one.map(Tuple1.apply)).toDF("text")
    two
  }
  private def processBigrams(fg: RDD[(String, Int)], bg:RDD[(String, Int)]):RDD[(String, (String, (Int, Int)))] = {
    val union = fg.fullOuterJoin(bg)
      .map(x => {
        var fgvalue = 0
        var bgvalue = 0
        if(x._2._1 != None){
          fgvalue = x._2._1.get
        }
        if(x._2._2 != None){
          bgvalue = x._2._2.get
        }
        (x._1, (fgvalue, bgvalue))
      })
      .reduceByKey((a,b)=> (a._1+b._1,a._2+b._2))
      .map(x=> {
        val bigram = x._1
        val words = bigram.split(" ")
        val count = x._2
        if (words.length > 1) {
          (words(0), (words(1), count))
        } else if (words.length == 1) {
          (words(0), (" ", count))
        } else {
          (" ", (" ", count))
        }
      })
    union
  }
  private def processUnigrams(fg: RDD[(String, Int)], bg: RDD[(String, Int)]): RDD[(String, (Int, Int))] = {
    val union = fg.fullOuterJoin(bg)
        .map(x=> {
          var fgvalue = 0
          var bgvalue = 0
          if(x._2._1 != None)
            fgvalue = x._2._1.get
          if(x._2._2 != None)
            bgvalue = x._2._2.get
          (x._1,(fgvalue,bgvalue))
        })
        .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    union

  }


}