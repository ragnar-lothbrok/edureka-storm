package com.edureka.storm.bolts;

import java.util.Map;

import com.edureka.storm.helpers.ProductRepository;
import com.edureka.storm.models.Product;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class CassandraBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Product product = ((Product) input.getValue(0));
		ProductRepository.insertProduct(product);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
