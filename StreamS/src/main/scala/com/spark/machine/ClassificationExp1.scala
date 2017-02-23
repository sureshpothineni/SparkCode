package com.spark.machine

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark._


object ClassificationExp1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Testing Reading")
    val sc = new SparkContext(conf)
  
    val input = sc.textFile("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/text8.txt", 8).map(line => line.split(" ").toSeq)
    
    val spam = sc.textFile("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/spam.txt")
    val normal = sc.textFile("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/normal.txt")
    // Create a HashingTF instance to map email text to vectors of 10,000 features.
    val tf = new HashingTF(numFeatures = 100)
    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    //Array((10000,[1468,4978,7403,7921,8409,9070,9552],[2.0,1.0,1.0,1.0,1.0,1.0,2.0]
    
    val idf = new IDF()
    val idfFeatures = idf.fit(spamFeatures)
    val tfIdfVectors = idfFeatures.transform(spamFeatures) 
    tfIdfVectors.collect()
    
    val word2vec = new Word2Vec()
    val model1 = word2vec.fit(input)
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
    
    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    // Array((1.0,(10000,[1468,4978,7403,7921,8409,9070,9552],[2.0,1.0,1.0,1.0,1.0,1.0,2.0])))
    
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.
    
    // Run Logistic Regression using the SGD algorithm.
    
    val model = new LogisticRegressionWithSGD().run(trainingData)
    // Test on a positive example (spam) and a negative one (normal).
    
    val posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    println("Prediction for positive test example: " + model.predict(posTest))
    println("Prediction for negative test example: " + model.predict(negTest))
    
  }
}

//71,77,79,454,3159,3290,3707,5678,6372,7023,9552
//3504,5405,6852,8365,9839
