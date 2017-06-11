package fr.univ_pau.kafkaandflink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

public class ReadFromKafka {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// setup Kafka sink

		ConfluentAvroDeserializationSchema deserSchema = new ConfluentAvroDeserializationSchema(
				"http://localhost:8081");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProps.setProperty("zookeeper.connect", "localhost:2181");
		kafkaProps.setProperty("group.id", "consumer1");
		FlinkKafkaConsumer08<String> flinkKafkaConsumer = new FlinkKafkaConsumer08<String>("avrotopic", deserSchema,
				kafkaProps);

		DataStream<String> kafkaStream = env.addSource(flinkKafkaConsumer);
		kafkaStream.rebalance().map(new MapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String map(String value) throws Exception {
				return "kafka say: " + value;
			}
		}).print();
		env.execute("Flink Kafka Java Example");
	}
}
