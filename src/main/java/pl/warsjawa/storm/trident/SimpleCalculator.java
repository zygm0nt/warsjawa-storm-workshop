package pl.warsjawa.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleCalculator {

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
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("x", "y"), 3,
                generateSampleData());
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        topology.newStream("spout1", spout)
                .parallelismHint(20)
                //.each(new Fields("x", "y", "added", "multiplied"), new PrintFilter())
                .each(new Fields("x", "y"), new PrintFilter())
                .each(new Fields("x", "y"), new AddAndMultiply(), new Fields("added", "multiplied"))
                .each(new Fields("x", "y", "added", "multiplied"), new PrintFilter())
                .each(new Fields("added", "multiplied"), new CheckEvenSum(), new Fields("evenType"))
                .partitionBy(new Fields("evenType"))
                .each(new Fields("evenType", "added", "multiplied"), new PrintFilter());

        return topology.build();
    }

    private static List[] generateSampleData() {
        Random random = new Random();
        List<Values> result = new ArrayList<Values>();
        for (int i = 0; i < 20; i++) {
            result.add(new Values(random.nextInt(100), random.nextInt(100)));
        }
        return result.toArray(new List[] {});
    }
}
