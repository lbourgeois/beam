package org.talend.beam.examples;

import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;

/**
 * Created by lbourgeois on 21/06/17.
 */
public class ExceptionHandlerDoFn<InputT extends Serializable, OutputT extends Serializable> extends DoFn<InputT, OutputT> {

    public final TupleTag<OutputT> mainOutput =
            new TupleTag<OutputT>("mainOutput") {
            };
    public final TupleTag<InputT> errorOutput =
            new TupleTag<InputT>("errorOutput") {
            };
    private transient DoFnInvoker fnInvoker;
    private DoFn wrappedDoFn;

    public ExceptionHandlerDoFn(DoFn wrappedDoFn) {

        super();
        this.wrappedDoFn = wrappedDoFn;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {

        fnInvoker = DoFnInvokers.invokerFor(wrappedDoFn);


        try {
            fnInvoker.invokeProcessElement(new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
                @Override
                public DoFn<InputT, OutputT>.ProcessContext processContext(
                        DoFn<InputT, OutputT> doFn) {
                    return processContext;
                }

                @Override
                public RestrictionTracker<?> restrictionTracker() {
                    throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
                }

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
        } catch (UserCodeException uce) {
            processContext.output(errorOutput, processContext.element());
        }
    }
}
