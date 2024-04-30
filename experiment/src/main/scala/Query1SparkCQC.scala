import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import graduateProject.basicLib.CqcConversions._
object Query1SparkCQC {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query1SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(args.head).map(row => {
			val f = row.split(" ")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		val cnt0=v1.count()
		val v2 = v1.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val longLessThan = (x: Long, y: Long) => x < y
		//g1+c1
		val v3 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v4 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v5 = v4.appendMf(v3)
		//group by new key
		val v6 = v5.reKeyBy(x => x(1).asInstanceOf[Int])
		val v7 = v6.groupBy()
		//index structure
		val v8 = v7.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(x, y)).persist()
		// mf column
		val v9 = v8.getMf((array:Array[Any])=>array(2))

		//g3+c2
		val v10 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v11 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v12 = v11.appendMf(v10)

		val v13 = v12.reKeyBy(x => x(0).asInstanceOf[Int])
		val v14 = v13.groupBy()

		val v15 = v14.sortWith[Long]((array:Array[Any])=>array(2).asInstanceOf[Long], (x: Long, y: Long) => longLessThan(y, x)).persist()

		val v16 = v15.getMf((array:Array[Any])=>array(2))
		//g2 add new column mf1
		val v17 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v18 = v17.appendMf(v9)
		//g2 add new column mf2
		val v19 = v18.reKeyBy(x => x(1).asInstanceOf[Int])
		val v20 = v19.appendMf(v16)
		val v21 = v20.reKeyBy(x => x(0).asInstanceOf[Int])
		//self comparison
		val v22 = v21.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(3).asInstanceOf[Long]))

		//enumerate phase
		val v23 = v22.map(t => (t._2(1).asInstanceOf[Int], Array(t._2(0), t._2(2))))
		val v24 = v23.enumerateWithOneComparison[Int,Long](v15,
			(array:Array[Any])=>array(1).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long],
			(x: Long, y: Long) => longLessThan(x, y), Array(), Array(0, 1, 2), (l, r) => (l(0).asInstanceOf[Int]))
		val v25 = v24.enumerateWithOneComparison[Int,Long](v8,
			(array:Array[Any])=>array(2).asInstanceOf[Long], (array:Array[Any])=>array(2).asInstanceOf[Long],
			(x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v25.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("original data cnt:" + cnt0)
		LOGGER.info("Query1-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query1-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
