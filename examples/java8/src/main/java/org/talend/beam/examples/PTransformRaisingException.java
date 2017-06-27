package org.talend.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by lbourgeois on 23/06/17.
 * Sample PTransform raising an exception when reading EXCEPTION word
 */
public class PTransformRaisingException extends PTransform<PCollection<String>, PCollection<String>> {

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(PTransformRaisingException.ExtractWordsFn.class, "emptyLines");

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
                    if (word.equals("EXCEPTION")) {
                        throw UserCodeException.wrap(new IllegalStateException("OUPS"));
                    }
                    c.output(word);
                }
            }
        }
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new PTransformRaisingException.ExtractWordsFn()));
    }
}
