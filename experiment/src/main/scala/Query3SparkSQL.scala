import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Query3SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query3SparkSQL")
        assert(args.length == 1)
        val path = args(0)
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
           " SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst "+
        "FROM Graph AS g1, Graph AS g2, Graph AS g3 "+
        "WHERE g1.dst = g2.src AND g2.dst = g3.src"

        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("original data cnt:" + cnt0)
        LOGGER.info("Query3-SparkSQL cnt: " + cnt)
        LOGGER.info("Query3-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}