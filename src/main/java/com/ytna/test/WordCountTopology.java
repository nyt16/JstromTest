package com.ytna.test;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yt.na on 2017/3/12.
 */
public class WordCountTopology {
    public static Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    public static void topology(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new Spout(), 1);
        builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new CountBolt(), 1).fieldsGrouping("split", new Fields("word"));

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
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(Integer.MAX_VALUE);
            cluster.shutdown();
        }
    }

    public static void main(String args[]) throws Exception{
        LOG.error("test/test/test");
        topology(args);
    }
}
