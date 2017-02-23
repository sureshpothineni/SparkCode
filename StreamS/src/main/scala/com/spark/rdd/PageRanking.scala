package com.spark.rdd

import org.apache.spark._
object PageRanking {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Testing Reading")
    val sc = new SparkContext(conf)

    //val links = sc.textFile("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/ranks.txt", 2)
    val link1 = sc.objectFile[(String, Seq[String])]("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/ranks.txt").partitionBy(new HashPartitioner(2))
    val links1 = sc.parallelize(List("p1",Seq("p1","p2","p3"), "p2",Seq("p4","p5")))
    //, ('p2', ['p1','p5']), ('p3', ['p1','p4']), ('p4', ['p5']) , ('p5', ['p1','p2','p3']
    //val links2 = links.map { x => (x(0) }
   
   
    val links = sc.parallelize(List(("p1", Seq("p2","p3","p4") ), ("p2", Seq("p1","p5")), ("p1", Seq("p1","p5"))) ).partitionBy(new HashPartitioner(2))
    var ranks = links.mapValues { x => 1.0 }
    
    val joins = links.join(ranks).flatMap{ 
      case (pageId, (links, rank)) => links.map(dest => (dest, rank/links.size))
    }
    
    //val joins2 = links.join(ranks).flatMap{ 
    //  case (pageId, (links, rank)) => links.map(dest => )
    // }
    
    val joins1 = links.join(ranks).map(f => (f._1, f._2))
    
  }
}  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
