package pl.warsjawa.storm.trident;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class StateLoggerFactory implements StateFactory {
    @Override
    public State makeState(Map conf, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        return new StateLogger ();
    }

}
