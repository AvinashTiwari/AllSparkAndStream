package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class SparkFirstUdfsample {
    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///d://tmp/")
                .getOrCreate();


        sparkSession.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);
        Dataset<Row> dataset = sparkSession.read().option("header",true)
                .csv("src/main/resources/exams/students.csv");

         // dataset= dataset.withColumn("pass", functions.lit(col("score").equalTo("A+")));
        dataset= dataset.withColumn("pass", callUDF("hasPassed", col("grade")));

        dataset.show();



        sparkSession.close();

    }

}
