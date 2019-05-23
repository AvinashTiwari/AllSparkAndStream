package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePricingAnlaysisfeature {

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
                .setInputCols(new String[] {"bedrooms", "bathrooms","sqft_living", "sqft_lot","floors","grade"})
                .setOutputCol("features");

        Dataset<Row> modelInput = vectorAssembler.transform(csvData)
                .select("price", "features")
                .withColumnRenamed("price", "label");
        modelInput.show();

        Dataset<Row>[] dataSplit = modelInput.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTestdata = dataSplit[0];
        Dataset<Row> holdoutData  =  dataSplit[1];
/*
        LinearRegressionModel linearRegressionModel = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .fit(trainingData);
        */

        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMaps = paramGridBuilder.addGrid(linearRegression.regParam(),new double[]{0.01, 0.5,0.1})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5,0.1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);
        TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestdata);
        LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();


        System.out.println("The training data r2 value is " + lrModel.summary().r2() + " and the RMSE is " + lrModel.summary().rootMeanSquaredError());

       System.out.println("The test r2 value is " + lrModel.evaluate(holdoutData).r2() + " and rsme value" +  lrModel.evaluate(holdoutData).rootMeanSquaredError());

        System.out.println("coefficients : " + lrModel.coefficients() + " intercept : " + lrModel.intercept());
        System.out.println("reg param : " + lrModel.getRegParam() + " elastic net param : " + lrModel.getElasticNetParam());


    }
}
