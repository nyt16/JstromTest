package com.ytna.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yt.na on 2017/3/12.
 */
public class CountBolt implements IRichBolt{

//    public static Logger LOG = LoggerFactory.getLogger(CountBolt.class);
    OutputCollector collector;
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        counts.put(word, ++count);
        System.out.println(counts);
        int t = 0;
//        BufferedWriter out = null;
//        try {
//            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("E:/storm.txt" , true)));
//
//            out.write(input.getSourceComponent()+"-->"+word+"\r\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (null != out) {
//                    out.close();
//                }
//            } catch (IOException io) {
//                io.printStackTrace();
//            }
//        }
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
