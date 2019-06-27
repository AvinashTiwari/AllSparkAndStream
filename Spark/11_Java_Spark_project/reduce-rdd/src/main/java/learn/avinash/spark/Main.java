package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
               List<Double> inputdata = new ArrayList<Double>();
               inputdata.add(35.5);
               inputdata.add(135.5);
               inputdata.add(235.5);
               inputdata.add(335.5);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myrdd = sc.parallelize(inputdata);
        Double result = myrdd.reduce(( value1,  value2) -> value1 + value2);

        System.out.println(result);

        sc.close();

    }
}
