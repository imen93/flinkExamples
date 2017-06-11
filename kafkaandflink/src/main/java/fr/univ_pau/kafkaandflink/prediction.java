package fr.univ_pau.kafkaandflink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

public static void main(String[] args) throws Exception {
        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// setup Kafka sink

		ConfluentAvroDeserializationSchema deserSchema = new ConfluentAvroDeserializationSchema(
				"http://localhost:8081");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProps.setProperty("zookeeper.connect", "localhost:2181");
		kafkaProps.setProperty("group.id", "consumer1");
		FlinkKafkaConsumer08<String> flinkKafkaConsumer = new FlinkKafkaConsumer08<String>("avrotopic", deserSchema,
				kafkaProps);
	// read from Kafka for example

		DataStream<String> kafkaStream = env.addSource(flinkKafkaConsumer);
        DataStream<Prediction> prediction = kafkaStream.map(new Predictor());

        prediction.print();

        env.execute();
    }

    public static class Predictor implements MapFunction<Value, Prediction>, CheckpointedFunction {

        private transient ListState<Model> modelState;

        private transient Model model;

        @Override
        public Prediction map(Value value) throws Exception {
            return model.predict(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // we don't have to do anything here because we assume the model to be constant
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Model> listStateDescriptor = new ListStateDescriptor<>("model", Model.class);

            modelState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);

            if (context.isRestored()) {
                // restore the model from state
                model = modelState.get().iterator().next();
            } else {
                modelState.clear();

                // read the model from somewhere, e.g. read from a file
                model = ...;

                // update the modelState so that it is checkpointed from now
                modelState.add(model);
            }
        }
    }

    public static class Model {}

    public static class Value{}

    public static class Prediction{}
}
