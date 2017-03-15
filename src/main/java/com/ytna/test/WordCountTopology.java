package com.ytna.test;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;


/**
 * Created by yt.na on 2017/3/12.
 */
public class WordCountTopology {
    public static Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    public static void topology(String[] args) throws Exception {

        BrokerHosts brokerHosts = new ZkHosts("10.30.10.174:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "obd_result_landu_check_info", "/kafka", "kafkaspout");
        spoutConfig.forceFromStart = false;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("split", new SplitBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("count", new CountBolt(), 2).fieldsGrouping("split", new Fields("word"));

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
            conf.setMaxTaskParallelism(2);
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
