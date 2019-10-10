package com.pwc.dataflow.example;

import javafx.scene.control.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

;

public class JavaDemo2 {
    private static final String PAYMENT_KIND = "Payment";

    static void pubSubToBigQuery(PubSubToBigQueryOptions options) {

        Pipeline pipeline = Pipeline.create(options);


        /*
         * Steps:
         *  1) Read messages in from Pub/Sub
         *  2) Transform the PubsubMessages into TableRows
         *     - Transform message payload via UDF
         *     - Convert UDF result to TableRow objects
         *  3) Write successful records out to BigQuery
         *  4) Write failed records out to BigQuery
         */

        /*
         * Step #1: Read messages in from Pub/Sub
         * Either from a Subscription or Topic
         */

//        PCollection<PubsubMessage> messages = null;
//        if (options.getUseSubscription()) {
//            messages =
//                    pipeline.apply(
//                            "ReadPubSubSubscription",
//                            PubsubIO.readMessagesWithAttributes()
//                                    .fromSubscription(options.getInputSubscription()));
//        } else {
//            messages =
//                    pipeline.apply(
//                            "ReadPubSubTopic",
//                            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
//        }


        pipeline.apply(
                "ReadPubSubTopic",
                PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                .apply(Window.into(FixedWindows.of(Duration.standardHours(options.getWindowSize()))))
                // Apply windowed file writes. Use a NestedValueProvider because the filename
                // policy requires a resourceId generated from the input value at runtime.
//                .apply(
//                        "Write File(s)",
//                        TextIO.write()
//                                .withWindowedWrites()
//                                .withNumShards(options.getNumShards())
//                                .to(
//                                        new WindowedFilenamePolicy(
//                                                options.getOutputDirectory(),
//                                                options.getOutputFilenamePrefix(),
//                                                options.getOutputShardTemplate(),
//                                                options.getOutputFilenameSuffix()))
//                                .withTempDirectory(ValueProvider.NestedValueProvider.of(
//                                        options.getOutputDirectory(),
//                                        (SerializableFunction<String, ResourceId>) input ->
//                                                FileBasedSink.convertToFileResourceIfPossible(input))));
                .apply("write ", TextIO.write().to(options.getOutput()));


        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PubSubToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBigQueryOptions.class);

        pubSubToBigQuery(options);
    }


    public interface PubSubToBigQueryOptions extends PipelineOptions {

        /**
         * Set this required option to specify where to write the output.
         */
//        @Description("Whether use the subscription")
//        @Required
//        boolean getUseSubscription();
//
//        void setUseSubscription(boolean value);
//
//        @Description("Pub/sub subscription to read data from")
//        @Required
//        String getInputSubscription();
//
//        void setInputSubscription(String value);

        @Description("Pub/sub topic to read data from")
        @Required
        String getInputTopic();

        void setInputTopic(String value);


        @Description("Big Query Table write to")
        String getOutputTableSpec();

        void setOutputTableSpec(String value);

        @Description("Big Query Table write to")
        int getWindowSize();
        void setWindowSize(int value);
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }



}
