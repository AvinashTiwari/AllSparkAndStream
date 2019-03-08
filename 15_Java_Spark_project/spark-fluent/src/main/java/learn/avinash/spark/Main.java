package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

         sc.parallelize(inputdata)
            .mapToPair(rawValue->new Tuple2<>(rawValue.split(":")[0],1L))
         .reduceByKey((value1, value2) -> value1 + value2)
                 .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2));;





        sc.close();

    }
}
