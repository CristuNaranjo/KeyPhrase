package org.wbs

import org.apache.spark.rdd.RDD

/**
  * Created by cristu on 16/09/16.
  */
object AnalyzeResult {

  def analyze (df: (RDD[(String, Double, Double, Double)],Tuple2[String,Int])):
//  RDD[(String,Double,Int)]
  Tuple2[Int,Double] = {
    val rdd = df._1
    val text = df._2._1
    val id = df._2._2
    var res = (0,0.0)

    //Filtro para quedarme solo con los bigram del texto FG y sus puntuaciones
    val bigramFiltered = rdd
      .filter(x => text.contains(x._1))
      .map(x => {
        val (bigram, score, phrase, inform) = x
        println("Phrase: " + phrase, " Inform: " + inform)
        (id,score)
      })
    //Calculo la probabilidad total de los bigram
    val bigramCount = bigramFiltered
        .reduceByKey((a,b) => (a + b))
    val count = bigramFiltered.count()
    if(count > 0){
      val scoreMean = bigramCount.first()._2/count
      res = (id, scoreMean)
    }else{
      res = (id, 0.0)
    }
    res
  }
}
