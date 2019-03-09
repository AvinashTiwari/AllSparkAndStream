package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
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

        JavaRDD<String> sentence =  sc.parallelize(inputdata);
        JavaRDD<String>  words =  sentence.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        JavaRDD<String> filterwords = words.filter(word-> word.length() > 1);
        words.foreach(System.out::println);
        filterwords.foreach(System.out::println);

        sc.close();

    }
}
