import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ERSparkRelationalTest {
    def main(args: Array[String]){
		val conf = new SparkConf().setAppName("Simple Application")
	        val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val tripleRDD = sc.textFile("/pengpeng/input/ER/ER_graph_v500K_l1K_edge1250K.txt")
		val schemaString = "obj pre sub"
		val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
		val schema = StructType(fields)
		val rowRDD = tripleRDD.map(_.split("\t")).map(attributes => Row(attributes(0), attributes(1), attributes(2).trim))
		val tripleDF = sqlContext.createDataFrame(rowRDD, schema)
		tripleDF.createOrReplaceTempView("triple")
		val results = sqlContext.sql("SELECT distinct T3.sub as v0 FROM triple  as T1, triple  as T2, triple  as T3 WHERE T1.pre=\"90\" and T1.obj=\"10827\" and T2.pre= \"9\" and T3.pre=\"93\" and T1.sub= T2.obj and T2.sub= T3.obj")
		//results.write.format("csv").save("/pengpeng/df.csv")
		results.show()
    }
}

