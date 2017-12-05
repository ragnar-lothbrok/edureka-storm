package com.edureka.storm;

import com.edureka.storm.bolts.ConsolePrintBolt;
import com.edureka.storm.bolts.GeoStatsBolt;
import com.edureka.storm.bolts.GeographyBolt;
import com.edureka.storm.bolts.RepeatVisitBolt;
import com.edureka.storm.bolts.VisitStatsBolt;
import com.edureka.storm.spout.ClickFileReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ClickStatsTopology {

	static final String TOPOLOGY_NAME = "click-stats-topology";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		if (args.length == 0) {
			System.out.println("Please enter click data file path and ip mapping file path.");
			throw new RuntimeException("Please enter logs file path.");
		}
//		args = new String[2];
//		args[0] = "/Users/raghugupta/Documents/Storm/click-stream.json";
//		args[1] = "/Users/raghugupta/Downloads/GeoIP2-City.mmdb";

		config.put("clickdatapath", args[0]);
		config.put("locationdbpath", args[1]);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("ClickFileReaderSpout", new ClickFileReaderSpout());

		builder.setBolt("GeographyBolt", new GeographyBolt()).shuffleGrouping("ClickFileReaderSpout");

		builder.setBolt("GeoStatsBolt", new GeoStatsBolt()).fieldsGrouping("GeographyBolt", new Fields("COUNTRY"));

		builder.setBolt("ConsolePrintBolt1", new ConsolePrintBolt()).shuffleGrouping("GeoStatsBolt");

		builder.setBolt("RepeatVisitBolt", new RepeatVisitBolt()).fieldsGrouping("ClickFileReaderSpout",
				new Fields("url"));

		builder.setBolt("VisitStatsBolt", new VisitStatsBolt()).shuffleGrouping("RepeatVisitBolt")
				.globalGrouping("RepeatVisitBolt");

		builder.setBolt("ConsolePrintBolt2", new ConsolePrintBolt()).shuffleGrouping("VisitStatsBolt");

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
