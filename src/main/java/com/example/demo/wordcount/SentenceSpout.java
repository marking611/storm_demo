package com.example.demo.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 数据源
 * Created by makai on 2018/5/30.
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "Apache Storm is a free and open source distributed realtime computation system",
            "Storm makes it easy to reliably process unbounded streams of data",
            "doing for realtime processing what Hadoop did for batch processing",
            "Storm is simple", "can be used with any programming language",
            "and is a lot of fun to use"
    };
    private int index = 0;

    //open()方法中是ISpout接口中定义，在Spout组件初始化是被调用
    //open()接受三个参数：一个包含Storm配置的Map，一个TopologyContext对象，提供了topology中组件的信息,SpoutOutputCollector对象提供发射tuple的方法。
    //
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //nextTuple()方法是任何Spout实现的核心
    //Storm调用这个方法，向输出的collector发出tuple
    @Override
    public void nextTuple() {
        if (index >= sentences.length){
            return;
        }
        //发送字符串
        this.collector.emit(new Values(sentences[index++]));
        Utils.sleep(1);
    }

    //declareOutputFields()是在IComponent接口中定义的，所有Storm的组件（spout和bolt）都必须实现这个接口
    //用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //定义输出字段描述
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
