package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
               List<String> inputdata = new ArrayList<String>();
               inputdata.add("WARN: Tuesday 4 September 0405");
               inputdata.add("WARN: Tuesday 4 September 0406");
               inputdata.add("ERROR: Tuesday 4 September 0408");
               inputdata.add("FATAL: Wednesday 5 September 1632");
               inputdata.add("ERROR: Friday 7 September 1854");
               inputdata.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessage = sc.parallelize(inputdata);
        JavaPairRDD<String, Long> pairRdd =  originalLogMessage.mapToPair(rawValue-> {
           String[] colums =  rawValue.split(":");
           String level = colums[0];
           String date = colums[1];
            Tuple2<String, Long> stringStringTuple2 = new Tuple2<>(level, 1L);
            return stringStringTuple2;

        });

      JavaPairRDD<String, Long> sumsRdd =  pairRdd.reduceByKey((value1, value2) -> value1 + value2);
        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2));



        sc.close();

    }
}
