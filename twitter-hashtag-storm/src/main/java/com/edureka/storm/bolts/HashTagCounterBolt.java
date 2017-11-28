package com.edureka.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class HashTagCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;

	private Map<String, Integer> counter;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		counter = new HashMap<String, Integer>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String key = input.getString(0);

		if (!counter.containsKey(key)) {
			counter.put(key, 1);
		} else {
			Integer c = counter.get(key) + 1;
			counter.put(key, c);
		}
		this.collector.emit(new Values(key, counter.get(key)));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		for (Map.Entry<String, Integer> entry : counter.entrySet()) {
			System.out.println("Result: " + entry.getKey() + " : " + entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("default", (new Fields("tag", "count")));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
