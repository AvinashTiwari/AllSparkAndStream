package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class SparkPivtol {
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


        Dataset<Row> dataset = sparkSession.read().option("header",true).csv("src/main/resources/biglog.txt");

        dataset =  dataset.select(col("level"),
                date_format(col("datetime"),"MMMM").alias("month"),
                date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
      //  dataset =  dataset.groupBy("level").pivot("month").count();
       // dataset =  dataset.groupBy("level").pivot("monthnum").count();
        Object[] month = new Object[]{"January" , "February" , "March" , "April", "May",
                "June", "July", "August", "September", "October",
                "November", "December"};
        List<Object> columns = Arrays.asList(month);

        dataset =  dataset.groupBy("level").pivot("month",columns).count();


        dataset.show(100);





        sparkSession.close();

    }

}
