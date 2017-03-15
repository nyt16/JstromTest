package com.ytna.test;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by yt.na on 2017/3/12.
 */
public class SplitBolt implements IRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
        public void execute(Tuple input) {
        byte[] bytete = input.getBinary(0);
        try{
            String sentence = new String(bytete,"UTF-8");
            for (String word : sentence.split("\\s+")) {
                System.out.println(word);
                collector.emit(new Values(word));
            }
        }catch (Exception e){

        }
//        byte sentence = input.getByte(0);
//        System.out.println(bytete);
    }

    @Override
    public void cleanup() {
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
