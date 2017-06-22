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

import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.transforms.MapElements.via;


/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link handledDoFnExceptionPipeline}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class handledDoFnExceptionPipeline {

  public static void main(String[] args) {
    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline object with the options we defined above.
    Pipeline p = Pipeline.create(options);


      final List<String> LINES = Arrays.asList(
              "To be, or not to be: that is the EXCEPTION question: ",
              "Whether 'tis nobler in the mind to suffer ",
              "The slings and arrows of outrageous fortune, ",
              "Or to take arms against a sea of troubles, ");

      // Create a PCollection from in-memory data
      p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())

     // Apply a user defined DoFn
     .apply("ExtractWords", ParDo.of(new ExceptionHandlerDoFn<String,String>(new DoFnRaisingException())))

     // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
     // transform returns a new PCollection of key/value pairs, where each key represents a unique
     // word in the text. The associated value is the occurrence count for that word.
     .apply(Count.<String>perElement())

     // Apply a MapElements transform that formats our PCollection of word counts into a printable
     // string, suitable for writing to an output file.
     .apply("FormatResults", via(new SimpleFunction<KV<String, Long>, String>() {
                       @Override
                       public String apply(KV<String, Long> input) {
                         return input.getKey() + ": " + input.getValue();
                       }
                     }))

     // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
     // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
     // formatted strings) to a series of text files.
     //
     // By default, it will write to a set of files with names like wordcount-00001-of-00005
     .apply(TextIO.write().to("wordcounts"));

    // Run the pipeline.
    p.run().waitUntilFinish();
  }
}
