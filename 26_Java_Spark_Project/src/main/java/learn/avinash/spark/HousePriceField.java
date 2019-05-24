package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceField {
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

        //csvData.describe().show();
        csvData = csvData.drop("id", "date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");
        for(String col: csvData.columns()){
            System.out.println( "The Correlation between price and "  + col + " Is " +  csvData.stat().corr("price", col));
        }



    }
}
