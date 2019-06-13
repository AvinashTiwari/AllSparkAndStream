package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;


public class NewCourseRecommendation {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("GymComp")
                .config("spark.sql.warehouse.dir","file:///D:/temp/")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                .option("header", true)
                .option("inferSchema",true)
                .csv("src/main/resources/VPPcourseViews.csv");

        csvData.show();
        csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
     //   csvData = csvData.groupBy("userId").pivot("courseId").sum("proportionWatched");
        Dataset<Row>[] trainingAndHoldout = csvData.randomSplit(new double[]{0.9, 0.1});
        Dataset<Row> trainingData = trainingAndHoldout[0];
        Dataset<Row> holdOut = trainingAndHoldout[1];
     ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("courseId")
                .setRatingCol("proportionWatched");

        ALSModel model = als.fit(csvData);
        Dataset<Row> userRec = model.recommendForAllUsers(5);
        List<Row> userRecList = userRec.takeAsList(5);

        for(Row r :userRecList){
            int userid = r.getAs(0);
            String recs = r.getAs(1).toString();
            System.out.println("Recommend system " + recs);
        }

}
}
