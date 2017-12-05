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

public class RepeatVisitBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private Map<String, Void> map = new HashMap<>();
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		String clientKey = input.getStringByField("clientkey");
		String url = input.getStringByField("url");
		String key = url + ":" + clientKey;

		if (map.containsKey(key)) {
			collector.emit(input, new Values(clientKey, url, Boolean.FALSE.toString()));
		} else {
			map.put(key, null);
			collector.emit(input, new Values(clientKey, url, Boolean.TRUE.toString()));
		}

		collector.ack(input);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("clientkey", "url", "unique"));
	}
}
