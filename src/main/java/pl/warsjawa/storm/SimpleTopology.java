package pl.warsjawa.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author mcl
 */
public class SimpleTopology {

    private static boolean remoteDeploy = true;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomDataSpout(1000));
        builder.setBolt("tokenize", new SplittingBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new CountingBolt(), 4).fieldsGrouping("tokenize", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0 && "local".equals(args[0])) {
           remoteDeploy = false;
        }

        if (remoteDeploy) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology("defaultTopologyName", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("abc", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
