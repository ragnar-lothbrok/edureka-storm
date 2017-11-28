package com.edureka.storm.topology;

import com.edureka.storm.bolts.CallLogCounterBolt;
import com.edureka.storm.bolts.CallLogCreatorBolt;
import com.edureka.storm.bolts.CallLogDurationBolt;
import com.edureka.storm.bolts.ConsolePrintBolt;
import com.edureka.storm.spouts.CallLogReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MobileLogTopology {

	protected static final String TOPOLOGY_NAME = "MOBILE-LOG-ANALYSIS";

	public static void main(String[] args) {

		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("CallLogReaderSpout", new CallLogReaderSpout());

		builder.setBolt("CallLogCreatorBolt", new CallLogCreatorBolt()).shuffleGrouping("CallLogReaderSpout");

		builder.setBolt("CallLogCounterBolt", new CallLogCounterBolt()).fieldsGrouping("CallLogCreatorBolt",
				new Fields("call"));
		builder.setBolt("ConsolePrintBolt1", new ConsolePrintBolt()).shuffleGrouping("CallLogCounterBolt");

		builder.setBolt("CallLogDurationBolt", new CallLogDurationBolt()).fieldsGrouping("CallLogCreatorBolt",
				new Fields("call"));
		builder.setBolt("ConsolePrintBolt2", new ConsolePrintBolt()).shuffleGrouping("CallLogDurationBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});
	}

}
