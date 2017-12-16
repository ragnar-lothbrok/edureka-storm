package com.edureka.storm;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class SlidingWindowBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	@Override
	public void execute(TupleWindow inputWindow) {
		Map<String, Integer> wordCount = new LinkedHashMap<String, Integer>();
		for (Tuple tuple : inputWindow.get()) {
			String keyword = tuple.getValueByField("keyword").toString();
			if (wordCount.get(keyword) == null) {
				wordCount.put(keyword, 1);
			} else {
				wordCount.put(keyword, wordCount.get(keyword) + 1);
			}
		}
		wordCount = wordCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(10).collect(Collectors
				.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		collector.emit(new Values(wordCount));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}

}
