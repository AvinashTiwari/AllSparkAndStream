package learn.avinash.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkDStream {

    public static void main(String[] args)  throws InterruptedException{
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("viewFigure");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        Collection<String> topics = Arrays.asList("viewrecords");
        Map<String, Object> params= new HashMap<>();
        params.put("bootstrap.servers", "localhost:9092");
        params.put("key.deserializer", StringDeserializer.class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id", "spark-group");
        params.put("auto.offset.reset", "latest");
       // params.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, params));

        JavaDStream<String> result = stream.map(item -> item.value());
        result.print();

        sc.start();



       sc.awaitTermination();
    }
}
