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

public class PaymentTypeCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private Map<String, Map<String, Integer>> statsMap;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.statsMap = new HashMap<>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String paymentType = input.getStringByField(CommonLogParser.PAYMENT_TYPE);
		String date = input.getStringByField(CommonLogParser.TX_DATE);

		if (!statsMap.containsKey(date)) {
			if (statsMap.get(date) == null) {
				statsMap.put(date, new HashMap<>());
			}
		}
		if (statsMap.get(date).get(paymentType) == null) {
			statsMap.get(date).put(paymentType, 1);
		} else {
			statsMap.get(date).put(paymentType, statsMap.get(date).get(paymentType) + 1);
		}
		collector.emit(input, new Values(date, paymentType, statsMap.get(date).get(paymentType)));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("default",
				(new Fields(CommonLogParser.TX_DATE, CommonLogParser.PAYMENT_TYPE, CommonLogParser.COUNT)));
	}

}
