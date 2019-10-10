package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JasonStringToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(@Element String message, OutputReceiver<TableRow> out)
            throws IOException {

        Date nowTime = new Date(System.currentTimeMillis());
        SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd");
        String retStrFormatNowDate = sdFormatter.format(nowTime);

        TableRow row = new TableRow()
                .set("time", retStrFormatNowDate)
                .set("message", message);
        out.output(row);





    }

}
