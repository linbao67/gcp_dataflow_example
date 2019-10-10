package com.pwc.dataflow.example;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Map;

public class EntityToString extends DoFn<Entity, String> {

    @ProcessElement
    public void processElement(@Element Entity payment, OutputReceiver<String> out)
            throws IOException {

        Map<String, Value> propMap = payment.getPropertiesMap();

        // Grab all relevant fields
        String paymentId = propMap.get("payment_id").getKeyValue().toString();
        String paymentNumber = propMap.get("payment_number").getStringValue();
        String accountId = propMap.get("account_id").getStringValue();
        String accountNumber = propMap.get("account_number").getStringValue();
        String accountName = propMap.get("account_name").getStringValue();
        Double amount = propMap.get("amount").getDoubleValue();
        String effectiveDate = propMap.get("effective_date").getTimestampValue().toString();

        String jsonString = String.format("{payment_id:%s,payment_number:%s,account_id:%s,account_number:%s,account_name:%s,amount:%.4f,effective_date:%s}",
                paymentId,paymentNumber,accountId,accountNumber,accountName,amount,effectiveDate);

        out.output(jsonString);




    }

}
