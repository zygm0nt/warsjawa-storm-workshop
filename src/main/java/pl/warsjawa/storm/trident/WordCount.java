package pl.warsjawa.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class WordCount {

    private static boolean remoteDeploy = true;

    public static void main(String[] args) throws Exception {
        StormTopology topology = buildTopology();

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0 && "local".equals(args[0])) {
            remoteDeploy = false;
        }

        if (remoteDeploy) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology("defaultTopologyName", conf, topology);
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("abc", conf, topology);

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }

    private static StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .aggregate(new Fields("word"), new Count(), new Fields("count"))
                .each(new Fields("word", "count"), new PrintFilter())
                .parallelismHint(6);
        return topology.build();
    }
}
