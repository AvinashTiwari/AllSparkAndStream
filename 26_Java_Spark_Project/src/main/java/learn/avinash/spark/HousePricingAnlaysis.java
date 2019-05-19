package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePricingAnlaysis {

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
                .csv("src/main/resources/kc_house_data.csv");

        csvData.printSchema();
        csvData.show();

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[] {"bedrooms", "bathrooms","sqft_living"})
                .setOutputCol("features");

        Dataset<Row> modelInput = vectorAssembler.transform(csvData)
                .select("price", "features")
                .withColumnRenamed("price", "label");
        modelInput.show();

        Dataset<Row>[] taraingTestData = modelInput.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = taraingTestData[0];
        Dataset<Row> testData  =  taraingTestData[1];

        LinearRegressionModel linearRegressionModel = new LinearRegression()
                .fit(trainingData);
        linearRegressionModel.transform(testData)
        .show();




    }
}
