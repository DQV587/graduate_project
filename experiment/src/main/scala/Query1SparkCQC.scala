import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import graduateProject.basicLib.CqcConversions._

object Query1SparkCQC {
	val LOGGER = LoggerFactory.getLogger("SparkCqcQuery")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		assert(args.length == 2)
		val path = args(0)
		val k = args(1).toInt
		conf.setAppName("CqcQueryProcess")
		val spark = SparkSession.builder.config(conf).getOrCreate()
		val Graph = spark.sparkContext.textFile(path).map(row => {
			val f = row.split(" ")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		Graph.count()
		val GraphAggregatedWithValue0ByCOUNT = Graph.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		GraphAggregatedWithValue0ByCOUNT.count()
		val comparisonFunc1 = (v8: Long, v10: Long) => v8 < v10
		val sortFunc1 = (x: Long, y: Long) => x < (y+k)
		val rdd1 = GraphAggregatedWithValue0ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd2 = Graph.keyBy(x => x(1).asInstanceOf[Int])
		val rdd3 = rdd2.appendMf(rdd1)
		val rdd4 = GraphAggregatedWithValue0ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd5 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd6 = rdd5.appendMf(rdd4)
		val rdd7 = rdd6.reKeyBy(x => x(1).asInstanceOf[Int])
		val rdd8 = rdd7.groupBy()
		val rdd9 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd10 = rdd8.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => sortFunc1(x,y))
		val rdd11 = rdd10.getMf((array:Array[Any])=>array(2).asInstanceOf[Long])
		val rdd12 = rdd9.appendMf(rdd11)
		val rdd13 = rdd12.reKeyBy(x => x(1).asInstanceOf[Int])
		val rdd14 = rdd13.groupBy()
		val rdd15 = rdd3.reKeyBy(x => x(0).asInstanceOf[Int])
		val rdd16 = rdd14.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => sortFunc1(x,y))
		val rdd17 = rdd16.getMf((array:Array[Any])=>array(2).asInstanceOf[Long])
		val rdd18 = rdd15.appendMf(rdd17)
		val rdd19 = rdd18.filter(x => comparisonFunc1(x._2(3).asInstanceOf[Long], x._2(2).asInstanceOf[Long]))
		val rdd20 = rdd19.enumerateWithOneComparison[Int,Long](rdd16, (array:Array[Any])=>array(2).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => comparisonFunc1(y, x), Array(0,1,2,3), Array(0))
		val rdd21 = rdd20.reKeyBy(x => x(4).asInstanceOf[Int])
		val rdd22 = rdd21.enumerateWithOneComparison[Int,Long](rdd10, (array:Array[Any])=>array(2).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => comparisonFunc1(y, x), Array(0,1,2,3,4), Array(0))
		val result = rdd22

		val ts1 = System.currentTimeMillis()
		val cnt = result.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("QueryProcess-SparkCQC cnt: " + cnt)
		LOGGER.info("QueryProcess-SparkCQC time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
