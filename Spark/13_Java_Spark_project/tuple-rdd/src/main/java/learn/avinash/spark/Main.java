package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
               List<Integer> inputdata = new ArrayList<Integer>();
               inputdata.add(35);
               inputdata.add(135);
               inputdata.add(235);
               inputdata.add(335);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalInteger = sc.parallelize(inputdata);

        JavaRDD< Tuple2<Integer, Double>> sqrtRDD = originalInteger.map(value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));



        sc.close();

    }
}
