package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initalRdd = sc.textFile("src/main/resources/subtitles/input.txt");
       JavaRDD<String> lettersOnlyRdd =  initalRdd.map(sentence -> sentence.replace("[^a-zAZ\\s]", "").toLowerCase());
        JavaRDD<String> removeBlankLine = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removeBlankLine.flatMap(sentence-> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> interstingWords = justWords.filter(word->Util.isBoring(word));

        List<String> results = interstingWords.take(50);

        results.forEach(strValue -> System.out.println(strValue));



        sc.close();

    }
}
