package com.example.demo.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 生成报告
 * Created by makai on 2018/5/30.
 */
public class ReportBolt extends BaseRichBolt {

    private HashMap<String, Long> counts = null;//保存单词和对应的计数

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
        //实时输出
        System.out.println("结果:"+this.counts);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //这里是末端bolt，不需要发射数据流，这里无需定义
    }

    //cleanup是IBolt接口中定义
    //Storm在终止一个bolt之前会调用这个方法
    @Override
    public void cleanup() {
        System.out.println("---------- FINAL COUNTS -----------");

        ArrayList<String> keys = new ArrayList();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key : keys){
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("----------------------------");
    }
}
