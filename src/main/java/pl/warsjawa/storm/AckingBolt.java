package pl.warsjawa.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author mcl
 */
public class AckingBolt extends BaseRichBolt {

    private final Logger log = LoggerFactory.getLogger("CUSTOM_LOG_FILE");
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getStringByField(SplittingBolt.FIELD_CONTENT);
        boolean isValid = tuple.getBooleanByField(SplittingBolt.FIELD_IS_VALID);
        if (isValid) {
            _collector.ack(tuple);
        } else {
            _collector.fail(tuple);
        }
        log.info("msg: {} valid: {}", msg, isValid);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
