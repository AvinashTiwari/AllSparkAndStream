package learn.spark.decision;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class VVPDecisionTree {

    public static UDF1<String, String> countryGrouping = new UDF1<String, String>() {

        @Override
        public String call(String country) throws Exception {
            List<String> topCountries = Arrays.asList(new String[]{"GB", "US", "IN", "UNKNOWN"});
            List<String> europeanCountries = Arrays.asList(new String[]{"BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE", "CH", "IS", "NO", "LI", "EU"});

            if (topCountries.contains(country)) return country;
            if (europeanCountries.contains(country)) return "EUROPE";
            else return "OTHER";
        }

    };

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("GymComp")
                .config("spark.sql.warehouse.dir", "file:///D:/temp/")
                .master("local[*]")
                .getOrCreate();

        spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);

        Dataset<Row> csvData = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/vppFreeTrials.csv");

        csvData = csvData.withColumn("country", callUDF("countryGrouping", col("country")))
                .withColumn("label", when(col("payments_made").geq(1), lit(1)).otherwise(lit(0)));

        StringIndexer countryIndex = new StringIndexer();
        csvData = countryIndex.setInputCol("country")
                .setOutputCol("countryIndex")
                .fit(csvData)
                .transform(csvData);

        Dataset<Row> countryIndexs = csvData.select("countryIndex");
        IndexToString indexToString = new IndexToString();

        countryIndexs.show();
    }
}
