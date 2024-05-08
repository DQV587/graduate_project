import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import graduateProject.basicLib.CqcConversions._

object Query3SparkCQC {
	val LOGGER = LoggerFactory.getLogger("SparkCqcQuery")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("CqcQueryProcess")
		assert(args.length == 1)
		val spark = SparkSession.builder.config(conf).getOrCreate()
		val Graph = spark.sparkContext.textFile(args.head).map(row => {
			val f = row.split(" ")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		Graph.count()
		val rdd1 = Graph.keyBy(x => x(1).asInstanceOf[Int])
		val rdd2 = rdd1.groupBy()
		val rdd3 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd4 = rdd3.semiJoin(rdd2)
		val rdd5 = rdd4.reKeyBy(x => x(1).asInstanceOf[Int])
		val rdd6 = rdd5.groupBy()
		val rdd7 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd8 = rdd7.semiJoin(rdd6)
		val rdd9 = rdd8.enumerateWithNoComparison[Int](rdd6, Array(0,1), Array(0))
		val rdd10 = rdd9.reKeyBy(x => x(2).asInstanceOf[Int])
		val rdd11 = rdd10.enumerateWithNoComparison[Int](rdd2, Array(0,1,2), Array(0))
		val result = rdd11

		val ts1 = System.currentTimeMillis()
		val cnt = result.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("QueryProcess-SparkCQC cnt: " + cnt)
		LOGGER.info("QueryProcess-SparkCQC time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
