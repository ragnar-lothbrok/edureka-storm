package com.edureka.storm.bolts;

import java.util.Map;

import com.edureka.storm.helpers.MySqlHelper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MySQLDumpBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private MySqlHelper mySqlHelper;

	private OutputCollector collector;

	public MySQLDumpBolt(String ip, String database, String username, String password) {
		this.mySqlHelper = new MySqlHelper(ip, database, username, password);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		mySqlHelper.persist(input);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		mySqlHelper.close();
	}

}
