package org.talend.beam.examples;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by lbourgeois on 26/06/17.
 */
public class ExceptionHandlerPTransform extends PTransform<PCollection<String>, PCollection<String>> {

    private PTransform<PCollection<String>, PCollection<String>> wrappedPTransform;

    public ExceptionHandlerPTransform(PTransform wrappedPTransform) {
        this.wrappedPTransform = wrappedPTransform;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        PCollection<String> result = null;
        try {
            result = wrappedPTransform.expand(input);
        } catch (IllegalStateException ise) {
            System.out.println("Illegal state exception caught");
        }
        return result;
    }
}
