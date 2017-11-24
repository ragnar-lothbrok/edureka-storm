package com.edureka.storm.topology;

import com.edureka.storm.bolts.StopWordsBolt;
import com.edureka.storm.bolts.WordCounterBolt;
import com.edureka.storm.spout.CSVFileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TrendingKeywordsTopology {

	static final String TOPOLOGY_NAME = "storm-trending-words";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		if (args.length == 0) {
			System.out.println("Please enter keywords file name.");
			throw new RuntimeException("Please enter keywords file path.");
		}

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("CSVFileSpout", new CSVFileSpout(args[0]));
		b.setBolt("StopWordsBolt", new StopWordsBolt()).shuffleGrouping("CSVFileSpout");
		b.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 10)).shuffleGrouping("StopWordsBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}
}
