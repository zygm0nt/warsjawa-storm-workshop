package pl.warsjawa.storm.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class PrintFilter implements Filter {

    private final Logger log = LoggerFactory.getLogger("CUSTOM_LOG_FILE");

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        log.info(tuple.toString());
        return true;
    }
}