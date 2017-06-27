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

import org.apache.beam.examples.DebuggingWordCount;
import org.apache.beam.examples.WindowedWordCount;
import org.apache.beam.examples.WordCount;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.transforms.MapElements.via;

/**
* Simple pipeline with handled exception from DoFn
*/
public class handledDoFnExceptionPipeline {

    public static void main(String[] args) {

        // Pipeline creation
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        // Sample input data
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the EXCEPTION question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        // DoFnRaisingException raises exception when reading EXCEPTION word
        ExceptionHandlerDoFn<String, String> exceptionHandlerDoFn = new ExceptionHandlerDoFn<String, String>(new DoFnRaisingException());

        // Apply the DoFn on the sample data with 2 outputs : one for the good words and one for the bad words (when exception raised)
        PCollectionTuple results = p.apply("Create", Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply("ExtractWords", ParDo.of(exceptionHandlerDoFn)
                        .withOutputTags(exceptionHandlerDoFn.mainOutput, TupleTagList.of(exceptionHandlerDoFn.errorOutput)));

        // Apply a count on good words, format for output and output to goodwords file
        results.get(exceptionHandlerDoFn.mainOutput).setCoder(StringUtf8Coder.of())
                .apply(Count.<String>perElement())
                .apply("FormatResults", via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))
                .apply(TextIO.write().to("goodwords"));

        // Output bad words from errorOutput to badwords file
        results.get(exceptionHandlerDoFn.errorOutput).setCoder(StringUtf8Coder.of())
                .apply(TextIO.write().to("badwords"));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }
}
