package org.talend.beam.examples;

import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Created by lbourgeois on 22/06/17.
 */
public class ExceptionHandlerArgumentProvider<InputT,OutputT> implements DoFnInvoker.ArgumentProvider<InputT,OutputT> {

    private DoFn<InputT, OutputT>.ProcessContext processContext;

    public ExceptionHandlerArgumentProvider(DoFn<InputT, OutputT>.ProcessContext processContext) {

        this.processContext = processContext;
    }

    @Override
    public BoundedWindow window() {
        return null;
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
        return null;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
        return null;
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
        return null;
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
        return null;
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
        return null;
    }

    @Override
    public State state(String stateId) {
        return null;
    }

    @Override
    public Timer timer(String timerId) {
        return null;
    }
}
