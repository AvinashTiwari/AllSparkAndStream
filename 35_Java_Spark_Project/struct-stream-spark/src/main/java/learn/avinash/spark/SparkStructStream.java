package learn.avinash.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStructStream {

    public static void main(String[] args)  throws InterruptedException, StreamingQueryException {
        System.setProperty("hadoop.home.dir", "c:/winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("StructViewReport")
                .getOrCreate();
        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords").load();

        session.conf().set("spark.sql.shuffle.partitions","10");

        df.createOrReplaceGlobalTempView("view_figures");
        Dataset<Row> result = session.sql("select window, cast(value) as course_name, sum(5) as second_watched from view_figures group by window( timestamp, '1 minutes') , course_name");
        StreamingQuery query = result.writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .option("truncate", false)
                .option("numRows", 50)
                .start();

        query.awaitTermination();

    }
}
