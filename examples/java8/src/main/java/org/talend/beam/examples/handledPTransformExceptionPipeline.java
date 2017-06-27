/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

/**
 * Simple pipeline with handled exception from DoFn
 */
public class handledPTransformExceptionPipeline {

    public static void main(String[] args) {

        // Pipeline creation
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        // Sample input data
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the EXCEPTION mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        // DoFn which raise exception when reading EXCEPTION word
        PTransformRaisingException pTransformRaisingException = new PTransformRaisingException();
        ExceptionHandlerPTransform exceptionHandlerPTransform = new ExceptionHandlerPTransform(pTransformRaisingException);

        // Apply the PTransform on the sample data with 2 outputs : one for the good words and one for the bad words (when exception raised)
        PCollection<String> results = p.apply("Create", Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply("ExtractWords", exceptionHandlerPTransform);

        results.setCoder(StringUtf8Coder.of())
                .apply(TextIO.write().to("ptransformout"));

//        Run the pipeline.
//        try {
        p.run().waitUntilFinish();
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
    }
}
