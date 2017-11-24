package com.edureka.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StopWordsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6069146554651714100L;

	private Set<String> STOP_WORDS_LIST = new HashSet<String>(Arrays.asList(new String[] { "http", "https", "the",
			"you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about", "can",
			"was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
			"want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would", }));
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String keyword = (String) input.getValueByField("keyword");
		if (!STOP_WORDS_LIST.contains(keyword)) {
			collector.emit(new Values(keyword));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("keyword"));
	}
}
