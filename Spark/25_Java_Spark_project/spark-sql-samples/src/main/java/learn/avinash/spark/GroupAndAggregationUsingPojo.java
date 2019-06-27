package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class GroupAndAggregationUsingPojo {
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
      //  dataset.createOrReplaceTempView("logging_table");
        //Dataset<Row> result = sparkSession.sql("select level , date_format(datetime, 'MMMM') as month , " +
           //     " cast(first(date_format(datetime, 'M')) as int) as monthnum , count(1) as total" +
             //   " from logging_table group by level, month order by monthnum ");

        //result.createOrReplaceTempView("logging_table");


      //  result = sparkSession.sql("select level , month , monthnum, count(1) as total  " +
        //        "from logging_table group by level, month order by monthnum ");
       // dataset.selectExpr("level","date_format(datetime, 'MMMM') as month");
        dataset =  dataset.select(col("level"),
                date_format(col("datetime"),"MMMM").alias("month"),
                date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
        dataset = dataset.groupBy(col("level"),col("month"), col("monthnum")).count();
        dataset = dataset.orderBy(col("monthnum"),col("level"));
        dataset = dataset.drop(col("monthnum"));
        dataset.show(100);





        sparkSession.close();

    }

}
