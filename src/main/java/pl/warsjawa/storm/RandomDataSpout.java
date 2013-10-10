package pl.warsjawa.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mcl
 */
public class RandomDataSpout extends BaseRichSpout {

    private transient Iterator<String> sentences;
    private Log log = LogFactory.getLog(RandomDataSpout.class);

    private SpoutOutputCollector _collector;
    private final int _interval;

    public RandomDataSpout(int interval) {
        _interval = interval;
    }

    private String sentence() {
        if (sentences == null) {
            sentences = Iterables.cycle(loadDataFromFile("/sentences.txt")).iterator();
        }
        return sentences.next();
    }

    private List<String> loadDataFromFile(String resourcePath) {
        List<String> result = new ArrayList<String>();
        try {
            List<String> lines = IOUtils.readLines(getClass().getResourceAsStream(resourcePath));

            for (String line : lines) {
                String[] tokens = line.split("\\.");
                result.addAll(Arrays.asList(tokens));
            }
        } catch (IOException e) {
            log.error("Error reading file", e);
        }
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_CONTENT));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void ack(Object id) {
        log.info("ack file " + id);

    }
    @Override
    public void nextTuple() {
        String msgId = UUID.randomUUID().toString();
        log.info("emitting file " + msgId);
        _collector.emit(new Values(sentence()), msgId);

        try {
            Thread.sleep(_interval);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    public static String FIELD_CONTENT = "content";
}
