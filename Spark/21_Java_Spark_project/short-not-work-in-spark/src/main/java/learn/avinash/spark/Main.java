package learn.avinash.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

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
        JavaRDD<String> blankNewWord = justWords.filter(word ->word.trim().length() > 0);

        JavaRDD<String> justinterstingWords = blankNewWord.filter(word->Util.isBoring(word));
        JavaPairRDD<String, Long> pairRdd =justinterstingWords.mapToPair(word-> new Tuple2<String,Long>(word, 1L));
        JavaPairRDD<String, Long> total  = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
        JavaPairRDD<Long, String> switched = total.mapToPair(tuple -> new Tuple2<Long , String>(tuple._2,tuple._1));
        JavaPairRDD<Long, String> sorted  = switched.sortByKey(false);
        sorted.coalesce(1);
        sorted.foreach(strValue -> System.out.println(strValue));

       // List<Tuple2<Long,String>> results = sorted.take(50);

        //results.forEach(strValue -> System.out.println(strValue));



        sc.close();

    }
}
