package com.edureka.storm.spouts;

import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CallLogReaderSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private Random randomGenerator = new Random();

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		while (true) {
			int count = 0;
			while (count < 1000) {
				count++;
				String fromMobileNumber = "79393" + RandomStringUtils.randomNumeric(5);
				String toMobileNumber = "79393" + RandomStringUtils.randomNumeric(5);

				if (!fromMobileNumber.equals(toMobileNumber)) {
					Integer duration = randomGenerator.nextInt(60);
					this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
				}
			}
			Utils.sleep(1000);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("from", "to", "duration"));
	}

}
