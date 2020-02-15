import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkRelationalTest {
    def main(args: Array[String]){
		val conf = new SparkConf().setAppName("Simple Application")
	        val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val tripleRDD = sc.textFile("/pengpeng/input/LUBM/LUBM100K.nt")
		val schemaString = "sub pre obj"
		val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
		val schema = StructType(fields)
		val rowRDD = tripleRDD.map(_.split("\t")).map(attributes => Row(attributes(0), attributes(1), attributes(2).substring(0, attributes(2).length - 1).trim))
		val tripleDF = sqlContext.createDataFrame(rowRDD, schema)
		tripleDF.createOrReplaceTempView("triple")
		val results = sqlContext.sql("SELECT T1.sub as v0, T1.obj as v1, T2.obj as v2 FROM triple  as T1, triple  as T2 , triple  as T3 WHERE T3.pre=\"<http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre>\" and T3.obj=\"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre83>\" and T1.pre= \"<http://purl.org/ontology/mo/conductor>\"  and  T1.sub= T3.sub and T2.pre=\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\" and T2.sub= T3.sub")
		//results.write.format("csv").save("/pengpeng/df.csv")
		results.show()
    }
}
