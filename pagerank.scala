
import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import scala.xml._
object PageRank
{
	def main(args:Array[String])
	{
		val spark = SparkSession.builder.appName("PageRank").getOrCreate()
                //val SPARK_MASTER = "spark://ec2-54-158-141-105.compute-1.amazonaws.com:7077"
		val file = spark.read.textFile(args(0)).rdd;
                val items = file.map(line => line.split("\t")) 
                val myrdd = items.map( arr=> {
                    val title = arr(1)
                    val text = arr(2)
		    val strxml = XML.loadString(text)
                    val words = strxml.flatMap(line => line \\ "target")
		    val m = words.map(l => (l \\ "target").text).filterNot(line => line==title)	
                    (title,m)
                })
               // myrdd.saveAsTextFile("results/pagerank_v14")
                // links has the form of (url,neighbors)
/*
                val links = lines.map{ s =>
                    val parts = s.split("\\s+")
                    (parts(0), parts(1))
                }.distinct().groupByKey().cache()
*/
		var ranks = myrdd.mapValues(v => 1.0) //ranks have the form of (url,rank)
                val iterations = if (args.length > 1) args(1).toInt else 10 //Default interation is 

		for(i <- 1 to iterations)
		{
			val contribs = myrdd.join(ranks).flatMap{
				case (url,(neighbors,rank)) => neighbors.map(dest => (dest, rank/neighbors.size))
			}
			ranks = contribs.reduceByKey(_+_).mapValues(0.15+0.85*_)
		}
		ranks.saveAsTextFile(args(2))
	}
}

/*
val file = sc.textFile("subset.tsv")
val myrdd = file.flatMap(arr=> XML.loadString(arr.split("\t")(2).replaceFirst("^([\\W]+)<","<"))\\"article")
val yourdd = myrdd.flatMap(l=>(l\\"target").text)
val d = file.map(arr => (arr.split("\t")(0),yourdd))
yourdd.saveAsTextFile("result/pagerank_v1")
myrdd.saveAsTextFile("result/pagerank_v2")
*/