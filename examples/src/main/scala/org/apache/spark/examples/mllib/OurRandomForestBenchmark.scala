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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

// $example off$

object OurRandomForestBenchmark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OurRandomForest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/dna_libsvm2.txt")

    val nombreBoucle = 5

    var res = Array.fill[Double](nombreBoucle)(0.0)
    val start = System.nanoTime()
    for (i <- 1 to nombreBoucle) {
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))

      val numClasses = 3
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 100 // Use more in practice.
      val featureSubsetStrategy = "onethird" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 10
      val maxBins = 32

      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println(s"Test Error = $testErr")
      // println(s"Learned classification forest model:\n ${model.toDebugString}")

      res(i - 1) = testErr
    }
    val stop = System.nanoTime()
    val time = stop - start
    val tempsMoyen = time / nombreBoucle
    println(res.mkString(" "))
    val moyenneErreur = res.sum / res.size
    println(s"Erreur moyenne = $moyenneErreur")
    println(s"Temps Moyen = $tempsMoyen")
    sc.stop()
  }
}
