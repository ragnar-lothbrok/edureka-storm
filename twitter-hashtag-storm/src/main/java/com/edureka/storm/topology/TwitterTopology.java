package com.edureka.storm.topology;

import com.edureka.storm.bolts.HashTagReaderBolt;
import com.edureka.storm.bolts.HashTagCounterBolt;
import com.edureka.storm.spout.TwitterSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterTopology {

	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSpout", new TwitterSpout());
		b.setBolt("HashTagReaderBolt", new HashTagReaderBolt()).shuffleGrouping("TwitterSpout");
		b.setBolt("HashTagCounterBolt", new HashTagCounterBolt()).fieldsGrouping("HashTagReaderBolt",
				new Fields("hashtag"));
		b.setBolt("ConsolePrintBolt", new com.edureka.storm.bolts.ConsolePrintBolt())
				.shuffleGrouping("HashTagCounterBolt");

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
