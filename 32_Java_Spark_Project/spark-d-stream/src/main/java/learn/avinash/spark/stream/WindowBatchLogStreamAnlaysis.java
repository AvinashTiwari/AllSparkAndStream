package learn.avinash.spark.stream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class WindowBatchLogStreamAnlaysis {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("sparkStream");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));
        JavaReceiverInputDStream<String> inputdata = sc.socketTextStream("localhost", 8989);
        JavaDStream<String> results = inputdata.map(item -> item);
        JavaPairDStream<String, Long> pairDsStream = results.mapToPair(rawMessage -> new Tuple2<String, Long>(rawMessage.split(",")[0], 1L));
        JavaPairDStream<String, Long> paitDStream = pairDsStream.reduceByKeyAndWindow((x, y) -> x + y, Duration.apply(60));
        paitDStream.print();
        sc.start();
        sc.awaitTermination();
/*
        SparkSession spark = SparkSession.builder()
                .appName("GymComp")
                .config("spark.sql.warehouse.dir","file:///D:/temp/")
                .master("local[*]")
                .getOrCreate();
*/
    }
}
