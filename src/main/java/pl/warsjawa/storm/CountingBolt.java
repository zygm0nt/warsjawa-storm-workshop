package pl.warsjawa.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mcl
 */
public class CountingBolt extends BaseRichBolt {

    private final Logger log = LoggerFactory.getLogger("CUSTOM_LOG_FILE");

    private OutputCollector _collector;

    private Map<String, AtomicInteger> wordCounts;

    private long lastLogTimestamp = 0L;
    private static long LOGGING_INTERVAL = 5000;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        wordCounts = new HashMap<String, AtomicInteger>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(SplittingBolt.FIELD_WORD);
        incrementWordCount(word);
        _collector.ack(tuple);

        periodicLogging();
    }

    private void periodicLogging() {
        long now = new Date().getTime();
        if (now - lastLogTimestamp > LOGGING_INTERVAL) {
            lastLogTimestamp = now;

            logTop10();
        }
    }

    private void logTop10() {
        List<Map.Entry<String, AtomicInteger>> l = orderMapByValue(wordCounts);
        int limit = l.size() > 10 ? 10 : l.size();
        List<Map.Entry<String, AtomicInteger>> top10 = l.subList(0, limit);
        log.info("Current top10: {}", Joiner.on(", ").join(top10));
    }

    @VisibleForTesting
    List<Map.Entry<String, AtomicInteger>> orderMapByValue(Map<String, AtomicInteger> map) {
        List<Map.Entry<String, AtomicInteger>> l = new ArrayList<Map.Entry<String, AtomicInteger>>();
        l.addAll(map.entrySet());
        Collections.sort(l, new Comparator<Map.Entry<String, AtomicInteger>>() {
            @Override
            public int compare(Map.Entry<String, AtomicInteger> e1, Map.Entry<String, AtomicInteger> e2) {
                return e2.getValue().get() - e1.getValue().get();
            }
        });
        return l;
    }

    private void incrementWordCount(String word) {
        if (!wordCounts.containsKey(word)) {
            wordCounts.put(word, new AtomicInteger(0));
        }
        wordCounts.get(word).incrementAndGet();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
