package pl.warsjawa.storm.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CheckEvenSum extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        int added = tuple.getIntegerByField("added");
        int multiplied = tuple.getIntegerByField("multiplied");

        boolean isAddedEven = added % 2 == 0;
        boolean isMultipliedEven = multiplied % 2 == 0;

        EvenType evenType = getEvenType(isAddedEven, isMultipliedEven);
        collector.emit(new Values(evenType.name()));

    }

    private EvenType getEvenType(boolean addedEven, boolean multipliedEven) {
        EvenType evenType;
        if (addedEven && multipliedEven) {
            evenType = EvenType.BOTH;
        } else if (addedEven) {
            evenType = EvenType.ADDED;
        } else if (multipliedEven) {
            evenType = EvenType.MULTIPLIED;
        } else {
            evenType = EvenType.NONE;
        }
        return evenType;
    }

    enum EvenType {
        BOTH, NONE, ADDED, MULTIPLIED;
    }
}
