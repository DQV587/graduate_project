import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import graduateProject.basicLib.CqcConversions._

object Query3SparkCQC {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query3SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(args.head).map(row => {
			val f = row.split(" ")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		val cnt0=v1.count()
		val v2 = v1.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(f => (f(1), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()
		val longLessThan = (x: Long, y: Long) => x < y
		//g1+c1
		val v4 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v6 = v5.appendMf(v4)

		val v7 = v6.reKeyBy(x => x(1).asInstanceOf[Int])
		val v8 = v7.groupBy()
		val v9 = v8.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(x, y)).persist()

		val v10 = v9.getMf((array:Array[Any])=>array(2).asInstanceOf[Long])
		// c3
		val v11 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		// g3+c4+c2
		val v12 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v13 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v14 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v15 = v14.appendMf(v12)
		val v16 = v15.appendMf(v13)
		val v17 = v16.reKeyBy(x => x(0).asInstanceOf[Int])
		val v18 = v17.groupBy()
		// construct one dimension index
		val v19 = v18.sortByOneDimArray[Long, Long]((array:Array[Any])=>array(3).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(y, x), (x: Long, y: Long) => longLessThan(y, x))
		val v20 = v19.getMf
		// g2+g1.mf+c3+g3.mf
		// 01   2   3   4
		val v21 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v22 = v21.appendMf(v10)
		val v23 = v22.appendMf(v11)
		val v24 = v23.reKeyBy(x => x(1).asInstanceOf[Int])
		val v25 = v24.appendMf[Long, Long](v20, (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(y, x))
		val v26 = v25.reKeyBy(x => x(0).asInstanceOf[Int])
		val v27 = v26.filter(x => longLessThan(x._2(3).asInstanceOf[Long], x._2(4).asInstanceOf[Long]))

		val v28 = v27.map(t => (t._2(1).asInstanceOf[Int], Array(t._2(0), t._2(2), t._2(3))))
		val v29 = v28.enumerateWithTwoComparisons[Int,Long,Long](v19, (array:Array[Any])=>array(1).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], Array(2), Array(0, 1, 2, 3), (l, r) => (l(0).asInstanceOf[Int]))
		val v30 = v29.enumerateWithOneComparison[Int,Long](v9, (array:Array[Any])=>array(4).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2, 3, 4), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v30.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
