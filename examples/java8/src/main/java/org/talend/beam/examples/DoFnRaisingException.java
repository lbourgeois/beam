package org.talend.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.UserCodeException;

/**
 * Created by lbourgeois on 21/06/17.
 */
public class DoFnRaisingException extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(DoFnRaisingException.class, "emptyLines");

    @ProcessElement
    public void processElement(ProcessContext c) {
        if (c.element().trim().isEmpty()) {
            emptyLines.inc();
        }

        // Split the line into words.
        String[] words = c.element().split(ExampleUtils.TOKENIZER_PATTERN);

        // Output each word encountered into the output PCollection.
        for (String word : words) {
            if (!word.isEmpty()) {
                if (word.equals("EXCEPTION")){
                    throw UserCodeException.wrap(new Exception("DoFn Exception"));
                }
                c.output(word);
            }
        }
    }

}
