package com.soumet.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.util.UUID;


/**
 * Created by ajmssc on 2/13/16.
 * Description:
 */
public class KafkaFlinkProducer {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        messageStream.addSink(new FlinkKafkaProducer<String>("192.168.99.100:32769", "testing", new SerializationSchema<String, byte[]>() {
            @Override
            public byte[] serialize(String s) {
                return s.getBytes();
            }
        }));
        env.execute("Flink Kafka Producer");

    }

    private static class SimpleStringGenerator implements org.apache.flink.streaming.api.functions.source.SourceFunction<String> {
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (true) {
                Thread.sleep((long) (Math.random()*10));
                for (int i=0; i < Math.random()*1000; ++i) sourceContext.collect(new UUID((long) (Math.random() * 1000000), System.currentTimeMillis()).toString());
            }
        }

        @Override
        public void cancel() {
            System.out.println("Cancelling flow");
        }
    }
}
