package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;

public class UdfAndSqlString {
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

        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

        sparkSession.udf().register("monthNum", (String month) ->{
            java.util.Date inputDate = (input.parse(month));
           return Integer.parseInt(output.format(inputDate));
        },DataTypes.IntegerType);


        Dataset<Row> dataset = sparkSession.read().option("header",true).csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = sparkSession.sql("select level , date_format(datetime, 'MMMM') as month , " +
                " count(1) as total" +
                " from logging_table group by level, month order by monthNum(month) , level ");

        result.createOrReplaceTempView("logging_table");


        //  result = sparkSession.sql("select level , month , monthnum, count(1) as total  " +
        //        "from logging_table group by level, month order by monthnum ");
        result.show(100);





        sparkSession.close();



    }


}
