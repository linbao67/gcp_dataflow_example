package com.pwc.dataflow.example;

import com.google.datastore.v1.Query;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;

;

public class DataStoreToPubSub {
    private static final String PAYMENT_KIND = "Payment";

    public interface DataStoreToPubSubOptions extends PipelineOptions {

        /** Set this required option to specify where to write the output. */
        @Description("Pubsub topic to write data to")
        @Required
        String getPubsubWriteTopic();

        void setPubsubWriteTopic(String value);
    }


    public static DatastoreV1.Read read(PipelineOptions options) {
        DatastoreV1.Read read = DatastoreIO.v1().read().withProjectId(options.as(GcpOptions.class).getProject());

        String localDatastoreHost = System.getenv("DATASTORE_EMULATOR_HOST");
        if (localDatastoreHost != null) {
            read = read.withLocalhost(localDatastoreHost);
        }

        return read;
    }




    static void datastoreToPubsub(DataStoreToPubSubOptions options) {

        Query.Builder query = Query.newBuilder();
        query.addKindBuilder().setName(PAYMENT_KIND);
//        query.setFilter(makeFilter(KEY_PROPERTY, PropertyFilter.Operator.HAS_ANCESTOR,
//                makeValue(makeKey(GUESTBOOK_KIND, guestbookName))));
//        query.addOrder(makeOrder(DATE_PROPERTY, PropertyOrder.Direction.DESCENDING));

        Pipeline pipeline = Pipeline.create(options);
//        pipeline.apply("ReadLines",
//                read(options).withQuery(query.build()))
//                .apply("EntityToString", ParDo.of(new EntityToString()))
//                .apply("write ", TextIO.write().to(options.getOutput()));

        pipeline.apply("ReadLines",
                DatastoreIO.v1().read().withProjectId(options.as(GcpOptions.class).getProject()).withLiteralGqlQuery("SELECT * FROM Payment"))
                .apply("EntityToString", ParDo.of(new EntityToString()))
                .apply(PubsubIO.writeStrings()
                        .to(options.getPubsubWriteTopic()));;


        pipeline.run().waitUntilFinish();
    }


    public static void main(String[] args) {
        DataStoreToPubSubOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataStoreToPubSubOptions.class);

        datastoreToPubsub(options);
    }



}
