package com.edureka.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class VisitStatsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private int total = 0;
	private int uniqueCount = 0;
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		boolean unique = Boolean.parseBoolean(input.getStringByField("unique"));
		total++;
		if (unique)
			uniqueCount++;

		collector.emit(input, new Values(total, uniqueCount));
		collector.ack(input);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("total", "total_unique"));
	}
}
