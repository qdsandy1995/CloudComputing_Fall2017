

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object prgraphx
{
	def main(args:Array[String])
	{
		val spark = SparkSession.builder.appName("prgraphx").getOrCreate()
        val sc = spark.sparkContext
		val graph = GraphLoader.edgeListFile(sc,args(0))
		val pageRank = graph.pageRank(0.0001).vertices
		val users = sc.textFile(args(1)).map { l =>
        val fields = l.split(" ")
        (fields(0).toLong, fields(1))
        }
        val ranksByname = users.join(pageRank).map {
          case (id, (username, rank)) => (username, rank)
        }
		//val pageRank = graph.pregel(initialMessage, iter)((oldV,msgSum) => 0.15 + 0.85*msgSum, triplet => triplet.src.pr/triplet.src.deg, (msgA,msgB) => msgA + msgB)
		ranksByname.saveAsTextFile(args(2))
		spark.stop()
	} 
}



