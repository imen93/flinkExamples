public static void main(String[] args) throws Exception {
        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Value> input = env.socketTextStream(hostName, port); // read from Kafka for example

        DataStream<Prediction> prediction = input.map(new Predictor());

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
                model = /home/imen/Downloads/svmModel3minSplit66;

                // update the modelState so that it is checkpointed from now
                modelState.add(model);
            }
        }
    }

    public static class Model {}

    public static class Value{}

    public static class Prediction{}
}
