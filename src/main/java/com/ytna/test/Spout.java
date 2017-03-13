package com.ytna.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Map;
import java.util.Random;

/**
 * Created by yt.na on 2017/3/12.
 */
public class Spout implements IRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    int arrayPosition;
    int arrayLength;

//    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
//            "and every where that marry went the lamb was sure to go",
//            "one two three four five six seven eight nine ten",
//            "this is a test of the emergency broadcast system this is only a test",
//            "peter piper picked a peck of pickeled peppers",
//            "JStorm is a distributed and fault-tolerant realtime computation system.",
//            "Inspired by Apache Storm, JStorm has been completely rewritten in Java and provides many more enhanced features.",
//            "JStorm has been widely used in many enterprise environments and proved robust and stable.",
//            "JStorm provides a distributed programming framework very similar to Hadoop MapReduce.",
//            "The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API",
//            " which is fully compatible with Apache Storm API",
//            "and submit the composed Topology to a working JStorm instance.",
//            "Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph).",
//            "Different from Hadoop MapReduce, a JStorm topology runs 24 * 7",
//            "the very nature of its continuity abd 100% in-memory architecture ",
//            "has been proved a particularly suitable solution for streaming data and real-time computation.",
//            "JStorm guarantees fault-tolerance.",
//            "Whenever a worker process crashes, ",
//            "the scheduler embedded in the JStorm instance immediately spawns a new worker process to take the place of the failed one.",
//            " The Acking framework provided by JStorm guarantees that every single piece of data will be processed at least once."};
    private static final String[] CHOICES ={"a b","c d","a g"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        arrayPosition = JStormUtils.parseInt(conf.get("array.position"), 0);
        arrayLength = CHOICES.length;
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
//        int n = sendNumPerNexttuple;
//        while (--n >= 0) {
        if(arrayPosition<=arrayLength-1){
            String sentence = CHOICES[arrayPosition];
            _collector.emit(new Values(sentence));
            arrayPosition++;
        }

//        }

        Utils.sleep(1000);
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
