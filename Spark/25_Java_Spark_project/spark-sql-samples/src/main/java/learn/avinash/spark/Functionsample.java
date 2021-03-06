package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Functionsample {

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

        Dataset<Row> dataset = sparkSession.read().option("header", true)
                .csv("src/main/resources/exams/students.csv");

		Column subjectColumn = dataset.col("subject");
		Column yearColumn  = dataset.col("year");

		Dataset<Row> math = dataset.filter(subjectColumn.equalTo("Math").and(yearColumn.geq(2007)));

		math.show();
        sparkSession.close();
		

	}

}
