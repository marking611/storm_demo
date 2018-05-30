package com.example.demo.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 切割句子
 * Created by makai on 2018/5/30.
 * <p>
 * BaseRichBolt 是 IComponent和IBolt接口的实现
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;

    // prepare()方法类似于ISpout 的open()方法。
    //这个方法在blot初始化时调用，可以用来准备bolt用到的资源,比如数据库连接
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    //SplitSentenceBolt核心功能是在类IBolt定义execute()方法，这个方法是IBolt接口中定义。
    //每次Bolt从流接收一个订阅的tuple，都会调用这个方法。
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word : words) {
            //发送单词
            collector.emit(new Values(word));
        }
    }

    //splitSentenceBolt类定义一个元组流,每个包含一个字段(“word”)。
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //定义了传输到下一个bolt的字段描述
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
