package com.edureka.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import com.edureka.storm.helper.GeoIP2Location;
import com.edureka.storm.helper.GeoIP2Location.Location;
import com.edureka.storm.helper.IPLocationFactory;

public class GeographyBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private GeoIP2Location resolver;

	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		String ip = input.getStringByField("ip");
		Location location = resolver.resolve(ip);

		if (location != null) {
			String city = location.getCity();
			String country = location.getCountryName();

			collector.emit(input, new Values(country, city));
		}
		collector.ack(input);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		resolver = IPLocationFactory.create(((String)stormConf.get("locationdbpath")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("COUNTRY", "CITY"));
	}
}
