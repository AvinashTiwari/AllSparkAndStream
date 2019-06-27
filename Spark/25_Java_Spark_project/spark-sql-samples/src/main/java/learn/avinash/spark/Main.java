package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        //	JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession sparkSession = SparkSession.builder().appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///d://tmp/")
                .getOrCreate();
        sparkSession.conf().set("spark.sql.shuffle.partitions","12");

        Dataset<Row> dataset = sparkSession.read().option("header",true).csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> result = sparkSession.sql("select level , date_format(datetime, 'MMMM') as month , " +
                " cast(first(date_format(datetime, 'M')) as int) as monthnum , count(1) as total" +
                " from logging_table group by level, month order by monthnum ");


        result.createOrReplaceTempView("logging_table");

        //dataset =  dataset.select(col("level"),
        //      date_format(col("datetime"),"MMMM").alias("month"),
        //    date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
        //  dataset =  dataset.groupBy("level").pivot("month").count();
        // dataset =  dataset.groupBy("level").pivot("monthnum").count();


        //dataset =  dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
        //dataset = dataset.drop("monthnum");

        result.show(100);

        result.explain();
       // Scanner scanner = new Scanner(System.in);
        //scanner.nextLine();

        sparkSession.close();

    }



}
