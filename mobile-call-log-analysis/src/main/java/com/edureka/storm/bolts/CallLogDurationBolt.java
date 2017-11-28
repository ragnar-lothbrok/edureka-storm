package com.edureka.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CallLogDurationBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	Map<String, Integer> counterMap;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String call = tuple.getString(0);
		Integer duration = tuple.getInteger(1);

		if (!counterMap.containsKey(call)) {
			counterMap.put(call, duration);
		} else {
			Integer c = counterMap.get(call) + duration;
			counterMap.put(call, c);
		}
		collector.emit(new Values(call, counterMap.get(call)));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call", "duration"));
	}

}
