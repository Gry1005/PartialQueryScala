import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._  
import org.apache.spark.SparkConf  
import scala.collection.mutable.Map  
import org.apache.spark._  
import org.apache.spark.graphx._  
import org.apache.spark.rdd.RDD  
import scala.runtime.ScalaRunTime._
  
object ERSparkStarGraphTest{  
    def main(args:Array[String]){  
        val conf = new SparkConf().setAppName("Simple Application")  
        val sc = new SparkContext(conf)

	val queryArr: Array[(String, String)] = Array(("290", "?y3"), ("561", "l363385"))

		val edges: RDD[Edge[String]] = sc.textFile("/pengpeng/input/ER/ER_graph_v500K_l1K_edge1250K.txt").map { line => 
			val fields = line.split("\t") 
			Edge(fields(0).toLong, fields(2).toLong, fields(1))
		}
  
        val users = sc.textFile("/pengpeng/input/ER/ER_graph_v500K_uris.txt").map { line =>  
            val fields = line.split(",")  
            (fields(0).toLong,fields(1))//解析顶点数据:ID(一定转成Long型),URI  
        }  
  
        val myGraph=Graph.apply(users,edges)//重构图，顶点数据以users为准
		
		val initialMsg = ""
		def vprog(vertexId: VertexId, value: String, message: String): String = {
			if (message == initialMsg)
				value
			else
				value + "\t" + message
		}
		
		def sendMsg(triplet: EdgeTriplet[String, String]): Iterator[(VertexId, String)] = {
			Iterator((triplet.dstId, triplet.attr + "\t" + triplet.srcAttr))
		}
		
		def mergeMsg(msg1: String, msg2: String): String = msg1 + "\t" + msg2
		
		val sparqlprocessing = myGraph.pregel(initialMsg, 1, EdgeDirection.In)(vprog, sendMsg, mergeMsg)
		
		//sparqlprocessing.vertices.collect.foreach(println(_))
		def dealRes(res: (VertexId, String)) = {
			val fields = res._2.split("\t")

			var tagArr:Array[Boolean] = new Array[Boolean](queryArr.length)
			for (i <- 0 to tagArr.length - 1) {
				tagArr(i) = false
			}

			//println(res._1 + "\t" + stringOf(tagArr))
			if(fields.length > 1){
				for (i <- 1 to fields.length - 1 by 2) {
					for (j <- 0 to queryArr.length - 1) {
						if (queryArr(j)._2.charAt(0) == '?'){
							if (fields(i) == queryArr(j)._1){
								tagArr(j) = true
							}
						}else{
							if (fields(i) == queryArr(j)._1 && fields(i + 1) == queryArr(j)._2){
								tagArr(j) = true
							}
						}
					}
				}
				//println(res._1 + "\t" + stringOf(tagArr) + "\t" + queryArr(0)._1 + "\t" + queryArr(0)._2 + "\t" + stringOf(fields))
				var finalTag = true
				for (i <- 0 to tagArr.length - 1) {
					finalTag = finalTag && tagArr(i)
				}
				if (finalTag){
					println(res)
				}
			}
		}
		
		sparqlprocessing.vertices.collect.foreach(dealRes(_))
    }  
}  


