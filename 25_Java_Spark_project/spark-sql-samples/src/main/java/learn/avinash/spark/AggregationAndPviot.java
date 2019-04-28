package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class AggregationAndPviot {
    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///d://tmp/")
                .getOrCreate();

        /*InferSchema

        Dataset<Row> dataset = sparkSession.read().option("header",true)
              .option("inferSchema", true)
                .csv("src/main/resources/exams/students.csv");
*/
        Dataset<Row> dataset = sparkSession.read().option("header",true)
                .csv("src/main/resources/exams/students.csv");


        dataset = dataset.groupBy("subject").pivot("year").agg(functions.avg(
               round(col("score"),2)
        ),
                round(stddev("score"),2));

        dataset.show();



        sparkSession.close();

    }

}
