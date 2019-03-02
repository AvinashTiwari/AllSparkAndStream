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

        JavaRDD<Integer> myrdd = sc.parallelize(inputdata);
        Integer result = myrdd.reduce(( value1,  value2) -> value1 + value2);
        JavaRDD<Double> sqrtRDD = myrdd.map(value -> Math.sqrt(value));
        sqrtRDD.foreach(value -> System.out.println(value));

        System.out.println(result);
        System.out.println("Cout " + sqrtRDD.count());

       JavaRDD<Long> singeIntegerRdd =  sqrtRDD.map(value -> 1L);
        Long count = singeIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println(count);


        sc.close();

    }
}
