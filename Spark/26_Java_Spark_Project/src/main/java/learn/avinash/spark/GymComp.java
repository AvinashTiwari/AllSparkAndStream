package learn.avinash.spark;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class GymComp {
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
                .csv("src/main/resources/GymCompetition.csv");

        csvData.printSchema();
        csvData.show();

        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataRow = vectorAssembler.transform(csvData);
        Dataset<Row> modelInput = csvDataRow.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
        modelInput.show();

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model =  linearRegression.fit(modelInput);
        System.out.println("the model intercept " + model.intercept()  + "and coffecient " + model.coefficients());

        model.transform(modelInput).show();

    }
}
