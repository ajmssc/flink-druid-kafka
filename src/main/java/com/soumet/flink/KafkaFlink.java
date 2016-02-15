package com.soumet.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.ProcessingTime;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by ajmssc on 2/13/16.
 * Description:
 */
public class KafkaFlink {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "192.168.99.100:32768");
        props.setProperty("group.id", "flink");
        props.setProperty("bootstrap.servers", "192.168.99.100:32769");


        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>("testing", new SimpleStringSchema(), props));

        messageStream
                .rebalance()
                .map(s -> 1)
                .timeWindowAll(ProcessingTime.of(5, TimeUnit.SECONDS))
                .sum(0)
//                .map(s -> "Kafka and Flink says: " + s)
                .print();

        env.execute("Flink consumer");

    }
}
