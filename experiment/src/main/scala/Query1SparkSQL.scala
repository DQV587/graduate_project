import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Query1SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        assert(args.length == 2)
        val path = args(0)
        val k = args(1).toInt
        conf.setAppName("Query1SparkSQL")
        val spark = SparkSession.builder.config(conf).getOrCreate()

        val schema0 = "src INTEGER, dst INTEGER"
        val df0 = spark.read.format("csv")
            .option("delimiter", " ")
            .option("quote", "")
            .option("header", "false")
            .schema(schema0)
            .load(path).persist()
        val cnt0=df0.count()
        df0.createOrReplaceTempView("Graph")

        val result = spark.sql(
            s"SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst, " +
                "c1.cnt AS cnt1, c2.cnt AS cnt2 " +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2 " +
                "WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src " +
                s"AND c1.cnt < c2.cnt+$k"
        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("original data cnt: " + cnt0)
        LOGGER.info("Query1-SparkSQL cnt: " + cnt)
        LOGGER.info("Query1-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}