package com.ytna.test;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by yt.na on 2017/3/12.
 */
public class WordCountTopology {

    public static void topology(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new Spout(), 2);
        builder.setBolt("split", new SplitBolt(), 3).shuffleGrouping("spout");
        builder.setBolt("count", new CountBolt(), 3).fieldsGrouping("split", new Fields("words"));

        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            /*
             * run in local cluster, for test in eclipse.
             */
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
            Thread.sleep(Integer.MAX_VALUE);
            cluster.shutdown();
        }
    }

    public static void main(String args[]) throws Exception{
        topology(args);
    }
}
