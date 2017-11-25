package com.edureka.storm.log.topology;

import com.edureka.storm.log.bolts.ConsolePrintBolt;
import com.edureka.storm.log.bolts.ResponseStatusCountBolt;
import com.edureka.storm.log.parser.CommonLogParser;
import com.edureka.storm.log.spouts.FileLogSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class LogAnalysisTopology {

	static final String TOPOLOGY_NAME = "log-analysis-topology";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		if (args.length == 0) {
			System.out.println("Please enter logs file name.");
			throw new RuntimeException("Please enter logs file path.");
		}

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("FileLogSpout", new FileLogSpout(args[0], new CommonLogParser()));
		builder.setBolt("ResponseStatusCountBolt", new ResponseStatusCountBolt()).fieldsGrouping("FileLogSpout",
				new Fields("response"));

		builder.setBolt("ConsolePrintBolt", new ConsolePrintBolt()).shuffleGrouping("ResponseStatusCountBolt");

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
