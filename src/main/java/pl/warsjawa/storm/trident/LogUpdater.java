package pl.warsjawa.storm.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class LogUpdater extends BaseStateUpdater<StateLogger> {
    @Override
    public void updateState(StateLogger stateLogger, List<TridentTuple> tuples, TridentCollector collector) {
        stateLogger.logWord(null, 1);
    }
}
