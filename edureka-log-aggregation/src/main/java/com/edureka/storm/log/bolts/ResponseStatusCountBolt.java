package com.edureka.storm.log.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ResponseStatusCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private Map<Integer, Integer> counts;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		int statusCode = input.getIntegerByField("response");
		int count = 0;

		if (counts.containsKey(statusCode)) {
			count = counts.get(statusCode);
		}

		count++;
		counts.put(statusCode, count);

		collector.emit(input, new Values(statusCode, count));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("default", (new Fields("response", "count")));
	}

}
