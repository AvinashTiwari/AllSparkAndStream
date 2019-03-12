package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initalRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        initalRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value-> System.out.println(value));


        sc.close();

    }
}
