import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import graduateProject.basicLib.CqcConversions._

object Query2SparkCQC {
	val LOGGER = LoggerFactory.getLogger("SparkCqcQuery")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		assert(args.length==2)
		val path=args(0)
		val k=args(1).toInt
		conf.setAppName("CqcQueryProcess")
		val spark = SparkSession.builder.config(conf).getOrCreate()
		val Graph = spark.sparkContext.textFile(path).map(row => {
			val f = row.split(" ")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		Graph.count()
		val GraphAggregatedWithValue1ByCOUNT = Graph.map(f => (f(1), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		GraphAggregatedWithValue1ByCOUNT.count()
		val GraphAggregatedWithValue0ByCOUNT = Graph.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		GraphAggregatedWithValue0ByCOUNT.count()
		val comparisonFunc1 = (v8: Long, v10: Long) => v8 < (v10+k)
		val sortFunc1 = (x: Long, y: Long) => x < y
		val comparisonFunc2 = (v12: Long, v14: Long) => v12 < v14
		val sortFunc2 = (x: Long, y: Long) => x < y
		val rdd1 = GraphAggregatedWithValue0ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd2 = Graph.keyBy(x => x(1).asInstanceOf[Int])
		val rdd3 = rdd2.appendMf(rdd1)
		val rdd4 = GraphAggregatedWithValue0ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd5 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd6 = rdd5.appendMf(rdd4)
		val rdd7 = GraphAggregatedWithValue1ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd8 = rdd3.appendMf(rdd7)
		val rdd9 = GraphAggregatedWithValue0ByCOUNT.map(x => (x(0).asInstanceOf[Int], x(1)))
		val rdd10 = Graph.keyBy(x => x(0).asInstanceOf[Int])
		val rdd11 = rdd10.appendMf(rdd9)
		val rdd12 = rdd11.reKeyBy(x => x(1).asInstanceOf[Int])
		val rdd13 = rdd12.groupBy()
		val rdd14 = rdd13.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => sortFunc1(x,y))
		val rdd15 = rdd14.getMf((array:Array[Any])=>array(2).asInstanceOf[Long])
		val rdd16 = rdd6.appendMf(rdd15)
		val rdd17 = rdd16.reKeyBy(x => x(1).asInstanceOf[Int])
		val rdd18 = rdd17.groupBy()
		val rdd19 = rdd8.reKeyBy(x => x(0).asInstanceOf[Int])
		val rdd20 = rdd18.sortByOneDimArray[Long, Long]((array:Array[Any])=>array(3).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => sortFunc1(x,y), (x: Long, y: Long) => sortFunc2(x,y))
		val rdd21 = rdd20.getMf
		val rdd22 = rdd19.appendMf[Long, Long](rdd21, (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => comparisonFunc1(x,y))
		val rdd23 = rdd22.filter(x => comparisonFunc2(x._2(4).asInstanceOf[Long], x._2(3).asInstanceOf[Long]))
		val rdd24 = rdd23.enumerateWithTwoComparisons[Int,Long,Long](rdd20, (array:Array[Any])=>array(2).asInstanceOf[Long], (array:Array[Any])=>array(3).asInstanceOf[Long], Array(0,1,2,3,4), Array(0,3))
		val rdd25 = rdd24.reKeyBy(x => x(5).asInstanceOf[Int])
		val rdd26 = rdd25.enumerateWithOneComparison[Int,Long](rdd14, (array:Array[Any])=>array(2).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => comparisonFunc1(y, x), Array(0,1,2,3,4,5,6), Array(0))
		val result = rdd26

		val ts1 = System.currentTimeMillis()
		val cnt = result.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("QueryProcess-SparkCQC cnt: " + cnt)
		LOGGER.info("QueryProcess-SparkCQC time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
