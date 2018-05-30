package com.example.demo.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * 统计单词
 * Created by makai on 2018/5/30.
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;

    //存储单词和对应计数
    private Map<String, Long> counts = null;

    //大部分实例变量通常是在prepare()中进行实例化，这个设计模式是由topology的部署方式决定的
    //因为在部署拓扑时,组件spout和bolt是在网络上发送的序列化的实例变量。
    //如果spout或bolt有任何non-serializable实例变量在序列化之前被实例化(例如,在构造函数中创建)
    //会抛出NotSerializableException并且拓扑将无法发布。
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if (count == null) {
            count = 0l;
        }
        this.counts.put(word, ++count);
        this.collector.emit(new Values(word,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明一个输出流，其中tuple包括了单词和对应的计数，向后发射
        //其他bolt可以订阅这个数据流进一步处理
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
