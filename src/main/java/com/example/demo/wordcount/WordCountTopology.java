package com.example.demo.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * 单词统计拓扑
 * Created by makai on 2018/5/30.
 */
public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new SentenceSpout(),1);
        builder.setBolt("split",new SplitSentenceBolt(),2).shuffleGrouping("spout");
        builder.setBolt("count",new WordCountBolt(),2).fieldsGrouping("split",new Fields("word"));

        Config config = new Config();
        config.setDebug(false);
        if (args != null && args.length > 0){
            //集群模式
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else {
            //本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count",config,builder.createTopology());
            TimeUnit.SECONDS.sleep(10);
            cluster.shutdown();
        }
    }
}
