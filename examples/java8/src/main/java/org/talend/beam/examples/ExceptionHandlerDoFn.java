package org.talend.beam.examples;

import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Created by lbourgeois on 21/06/17.
 */
public class ExceptionHandlerDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private final DoFnInvoker fnInvoker;
    private DoFn<InputT, OutputT> wrappedDoFn;

    public ExceptionHandlerDoFn(DoFn wrappedDoFn) {

        super();
        wrappedDoFn = wrappedDoFn;
        fnInvoker = DoFnInvokers.invokerFor(wrappedDoFn);
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        fnInvoker.invokeProcessElement(       new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
            @Override
            public DoFn<InputT, OutputT>.ProcessContext processContext(
                    DoFn<InputT, OutputT> doFn) {
                return processContext;
            }

            @Override
            public RestrictionTracker<?> restrictionTracker() {
                throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
            }

            // Unsupported methods below.

            @Override
            public BoundedWindow window() {
                throw new UnsupportedOperationException(
                        "Access to window of the element not supported in Splittable DoFn");
            }

            @Override
            public StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
                throw new IllegalStateException(
                        "Should not access startBundleContext() from @"
                                + DoFn.ProcessElement.class.getSimpleName());
            }

            @Override
            public FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
                throw new IllegalStateException(
                        "Should not access finishBundleContext() from @"
                                + DoFn.ProcessElement.class.getSimpleName());
            }

            @Override
            public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(
                    DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException(
                        "Access to timers not supported in Splittable DoFn");
            }

            @Override
            public State state(String stateId) {
                throw new UnsupportedOperationException(
                        "Access to state not supported in Splittable DoFn");
            }

            @Override
            public Timer timer(String timerId) {
                throw new UnsupportedOperationException(
                        "Access to timers not supported in Splittable DoFn");
            }
        });
    }

}
