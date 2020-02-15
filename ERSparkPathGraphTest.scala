import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._  
import org.apache.spark.SparkConf  
import scala.collection.mutable.Map  
import org.apache.spark._  
import org.apache.spark.graphx._  
import org.apache.spark.rdd.RDD  
import scala.runtime.ScalaRunTime._
import scala.util.control._
  
object ERSparkPathGraphTest{  
    def main(args:Array[String]){  
        val conf = new SparkConf().setAppName("Simple Application")  
        val sc = new SparkContext(conf)

	val queryArr: Array[(String, String)] = Array(("93", "?y"), ("9", "?y"), ("90", "l10827"))

		val edges: RDD[Edge[String]] = sc.textFile("/pengpeng/input/ER/ER_graph_v40000_l100.txt").map { line => 
			val fields = line.split("\t") 
			Edge(fields(0).toLong, fields(2).toLong, fields(1))
		}
  
        val users = sc.textFile("/pengpeng/input/ER/ER_graph_v40000_uris.txt").map { line =>  
            val fields = line.split(",")  
            (fields(0).toLong,fields(1))//解析顶点数据:ID(一定转成Long型),URI  
        }  
  
        val oGraph=Graph.apply(users,edges)//重构图，顶点数据以users为准
		
		def subgraphFunc(epred: EdgeTriplet[String, String]): Boolean = {
			var tag = false
			val loop = new Breaks
			loop.breakable {
				for (i <- 0 to queryArr.length - 1) {
					if(queryArr(i)._1 == epred.attr){
						tag = true
						loop.break
					}
				}
			}
			tag
		}
		
		val myGraph=oGraph.subgraph(subgraphFunc)//reduce the graph
		
		val initialMsg = ""
		def vprog(vertexId: VertexId, value: String, message: String): String = {
			
			if (message == initialMsg)
				"1\t" + value
			else{
				//val content = value.substring(value.indexOf('\t')).trim
				//val curURI = content.substring(0, content.indexOf('\t')).trim
				//val iterNum = value.substring(0, value.indexOf('\t')).trim
				"1" + value + "\t" + message
			}
		}
		
		def sendMsg(triplet: EdgeTriplet[String, String]): Iterator[(VertexId, String)] = {
			val fields = triplet.srcAttr.trim.split("\t")
			//if (fields.length == 2 && fields(0).equals("1")) {
			//	Iterator.empty
			//}else{
				Iterator((triplet.dstId, fields(0) + "\t" + triplet.attr + "\t" + triplet.srcAttr.substring(triplet.srcAttr.indexOf("\t")).trim))
			//}
		}
		
		def mergeMsg(msg1: String, msg2: String): String = msg1 + "\t" + msg2
		
		val sparqlprocessing = myGraph.pregel(initialMsg, queryArr.length, EdgeDirection.In)(vprog, sendMsg, mergeMsg)
		
		//sparqlprocessing.vertices.collect.foreach(println(_))
		def dealRes(res: (VertexId, String)) = {
			
			val TermArr = res._2.split("\t")
			val stack = new Array[(String, String, String)](TermArr.length)
			var stack_top = 0;
			if(TermArr.length > 2){
				val loop = new Breaks
				loop.breakable {
					
					for (i <- 2 to TermArr.length - 1 by 3) {
						if(TermArr(i).charAt(0) == '1'){
							if(TermArr(i).length != 1){
								while (stack_top != 0 && TermArr(i).length >= stack(stack_top - 1)._1.length) {
									stack_top = stack_top - 1
								}
								stack(stack_top) = (TermArr(i), TermArr(i + 1), TermArr(i + 2))
								stack_top = stack_top + 1
							} else {
								var tagArr:Array[Boolean] = new Array[Boolean](queryArr.length)
								for (i <- 0 to tagArr.length - 1) {
									tagArr(i) = false
								}
								
								stack(stack_top) = (TermArr(i), TermArr(i + 1), TermArr(i + 2))
								stack_top = stack_top + 1
								
								for (j <- 0 to stack_top - 1) {
									if (queryArr(j)._2.charAt(0) == '?'){
										if (stack(j)._2 == queryArr(j)._1){
											tagArr(j) = true
										}
									}else{
										if (stack(j)._2 == queryArr(j)._1 && stack(j)._3 == queryArr(j)._2){
											tagArr(j) = true
										}
									}
								}
								
								//if (res._1 == 4){
								//	println(stack_top + "\t" + stringOf(tagArr) + "\t" + stringOf(stack))
								//}
								
								stack_top = stack_top - 1
								
								var finalTag = true
								for (i <- 0 to tagArr.length - 1) {
									finalTag = finalTag && tagArr(i)
								}
								if (finalTag){
									println(res)
									loop.break
								}
							}
						} else{
							loop.break
						}
					}
				}
			}
		}
		
		sparqlprocessing.vertices.collect.foreach(dealRes(_))
    }  
}
