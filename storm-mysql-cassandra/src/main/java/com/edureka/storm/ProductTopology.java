package com.edureka.storm;

import com.edureka.storm.bolts.CassandraBolt;
import com.edureka.storm.bolts.MySQLDumpBolt;
import com.edureka.storm.helpers.CassandraHelper;
import com.edureka.storm.helpers.ProductRepository;
import com.edureka.storm.schemes.ProductScheme;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class ProductTopology {

	static final String TOPOLOGY_NAME = "product-topology";

	public static void main(String[] args) {

		ZkHosts zkHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "product_topic", "/kafka", "product_topic");
		spoutConfig.scheme = new SchemeAsMultiScheme(new ProductScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("kafka-spout", kafkaSpout);

		ProductRepository.session = new CassandraHelper().getSession("Test Cluster", "edureka", "localhost");

		builder.setBolt("cassandra-bolt", new CassandraBolt()).shuffleGrouping("kafka-spout");

		builder.setBolt("sql-bolt", new MySQLDumpBolt("localhost:3306", "catalog", "root", "")).shuffleGrouping("kafka-spout");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, new Config(), builder.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}
}
