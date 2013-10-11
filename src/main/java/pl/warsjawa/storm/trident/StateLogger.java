package pl.warsjawa.storm.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;

import java.util.List;

public class StateLogger implements State, MapState<String> {

    private final Logger log = LoggerFactory.getLogger("CUSTOM_LOG_FILE");

    @Override
    public void beginCommit(Long txId) {
    }

    @Override
    public void commit(Long txId) {
    }


    public void logWord(String word, int count) {
        log.info("{} :  {}", word, count);
    }

    @Override
    public List<String> multiUpdate(List<List<Object>> lists, List<ValueUpdater> valueUpdaters) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> lists, List<String> ts) {
        log.info("abc");
    }

    @Override
    public List<String> multiGet(List<List<Object>> lists) {
        return null;
    }
}
