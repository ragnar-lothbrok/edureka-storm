package com.edureka.storm.order.bolts;

import java.util.HashMap;
import java.util.Map;

import com.edureka.storm.order.parser.CommonLogParser;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private Map<String, Integer> statsMap = new HashMap<>();

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.statsMap = new HashMap<>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String date = input.getStringByField(CommonLogParser.TX_DATE);

		if (!statsMap.containsKey(date)) {
			statsMap.put(date, 1);
		}
		statsMap.put(date, statsMap.get(date) + 1);
		collector.emit(input, new Values(date, statsMap.get(date)));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("default", (new Fields(CommonLogParser.TX_DATE, CommonLogParser.COUNT)));
	}

}
