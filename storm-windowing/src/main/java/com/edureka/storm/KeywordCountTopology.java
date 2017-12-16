package com.edureka.storm;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public class KeywordCountTopology {

	public static void main(String[] args) {

		Config config = new Config();
		config.setMessageTimeoutSecs(120);

//		if (args.length == 0) {
//			System.out.println("Please enter keywords file name.");
//			throw new RuntimeException("Please enter keywords file path.");
//
//		}
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("CSVFileSpout", new CSVFileSpout("/Users/raghugupta/Documents/Storm/keywords.csv"));

		builder.setBolt("slidingwindowbolt",
				new SlidingWindowBolt().withWindow(new Duration(5000, TimeUnit.MILLISECONDS), new Duration(2000, TimeUnit.MILLISECONDS)), 1)
				.shuffleGrouping("CSVFileSpout");
		
		builder.setBolt("ConsolePrintBolt", new ConsolePrintBolt()).shuffleGrouping("slidingwindowbolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(KeywordCountTopology.class.getSimpleName(), config, builder.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(KeywordCountTopology.class.getSimpleName());
				cluster.shutdown();
			}
		});
	}

}
