package com.edureka.storm.topology;

import com.edureka.storm.bolts.StopWordsBolt;
import com.edureka.storm.bolts.WordCounterBolt;
import com.edureka.storm.bolts.WordSplitterBolt;
import com.edureka.storm.spout.TwitterSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TwitterTopology {

	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSpout", new TwitterSpout());
		b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSpout");
		b.setBolt("StopWordsBolt", new StopWordsBolt()).shuffleGrouping("WordSplitterBolt");
		b.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 50)).shuffleGrouping("StopWordsBolt");

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
