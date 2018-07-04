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

object OurRandomForest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OurRandomForest")
    val sc = new SparkContext(conf)
    // do not print all the log
    sc.setLogLevel("INFO")
    // $example on$
    // Load and parse the data file.
    // val data = spark.read.format("com.databricks.spark.csv")
     // .option("header", "false")
     // .load("data/mllib/wave500k.txt").rdd

    // Load and parse the data file
    // val data = sc.textFile("data/mllib/wave500k.txt")
   // val parsedData = data.map { line =>
     // val parts = line.split(',')
     // LabeledPoint(parts(0), Vectors.dense(parts.tail))
   // }
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/wave500k_libsvm.txt")

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50 // Use more in practice.
    val featureSubsetStrategy = "onethird" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")

    // Save and load model
    model.save(sc, "target/tmp/ourRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    // $example off$

    sc.stop()
  }

}
