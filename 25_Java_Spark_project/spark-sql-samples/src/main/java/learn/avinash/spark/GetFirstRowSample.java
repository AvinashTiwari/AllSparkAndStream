package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GetFirstRowSample {

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

        dataset.show();
		long numberOfRow = dataset.count();
		System.out.println("Number of Rows " + numberOfRow);

        Row firstRow = dataset.first();
        String subject = firstRow.get(2).toString();
        System.out.println("Subject " + subject);

        String subjectAs = firstRow.getAs("subject").toString();
        System.out.println("Subject " + subjectAs);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("year " + year);

        sparkSession.close();
		

	}

}
