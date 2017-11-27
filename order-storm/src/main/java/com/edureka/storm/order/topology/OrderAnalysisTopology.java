package com.edureka.storm.order.topology;

import com.edureka.storm.order.bolts.ConsolePrintBolt;
import com.edureka.storm.order.bolts.OrderCountBolt;
import com.edureka.storm.order.bolts.PaymentTypeCountBolt;
import com.edureka.storm.order.bolts.StateStatusCountBolt;
import com.edureka.storm.order.bolts.CityStatusCountBolt;
import com.edureka.storm.order.parser.CommonLogParser;
import com.edureka.storm.order.spouts.FileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class OrderAnalysisTopology {

	static final String TOPOLOGY_NAME = "OrderAnalysisTopology";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		if (args.length == 0) {
			System.out.println("Please enter order file path.");
			throw new RuntimeException("Please enter order file path.");
		}

		// args = new String[1];
		// args[0] = "/Users/raghugupta/Documents/Storm/Module_8.1_transactions.csv";

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("FileSpout", new FileSpout(args[0], new CommonLogParser()));
		builder.setBolt("CityStatusCountBolt", new CityStatusCountBolt()).fieldsGrouping("FileSpout",
				new Fields(CommonLogParser.CITY));

		builder.setBolt("ConsolePrintBolt", new ConsolePrintBolt()).shuffleGrouping("CityStatusCountBolt");

		builder.setBolt("StateStatusCountBolt", new StateStatusCountBolt()).fieldsGrouping("FileSpout",
				new Fields(CommonLogParser.STATE));
		builder.setBolt("ConsolePrintBolt1", new ConsolePrintBolt()).shuffleGrouping("StateStatusCountBolt");

		builder.setBolt("OrderCountBolt", new OrderCountBolt()).fieldsGrouping("FileSpout",
				new Fields(CommonLogParser.STATE));
		builder.setBolt("ConsolePrintBolt3", new ConsolePrintBolt()).shuffleGrouping("OrderCountBolt");

		builder.setBolt("PaymentTypeCountBolt", new PaymentTypeCountBolt()).fieldsGrouping("FileSpout",
				new Fields(CommonLogParser.STATE));
		builder.setBolt("ConsolePrintBolt4", new ConsolePrintBolt()).shuffleGrouping("PaymentTypeCountBolt");

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
