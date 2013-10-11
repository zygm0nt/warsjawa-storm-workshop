package pl.warsjawa.storm.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class AddAndMultiply extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        int i1 = tuple.getInteger(0);
        int i2 = tuple.getInteger(1);
        collector.emit(new Values(i1 + i2, i1 * i2));
    }
}