package com.edureka.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagReaderBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6069146554651714100L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		for (HashtagEntity hashtage : tweet.getHashtagEntities()) {
			this.collector.emit(new Values(hashtage.getText()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}
}
