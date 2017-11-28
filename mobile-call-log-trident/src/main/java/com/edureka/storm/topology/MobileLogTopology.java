package com.edureka.storm.topology;

import com.edureka.storm.spouts.CallLogReaderSpout;
import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;

public class MobileLogTopology {

	protected static final String TOPOLOGY_NAME = "MOBILE-LOG-ANALYSIS-TRIDENT";

	public static void main(String[] args) {

		CallLogReaderSpout feederBatchSpout = new CallLogReaderSpout(ImmutableList.of("from", "to", "duration"));

		System.out.println("Mobile Log Analyser Trident");
		TridentTopology topology = new TridentTopology();

		TridentState callCounts = topology.newStream("CallLogReaderSpout", feederBatchSpout)
				.each(new Fields("from", "to"), new FormatCall(), new Fields("call")).groupBy(new Fields("call"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

		LocalDRPC drpc = new LocalDRPC();

		topology.newDRPCStream("call_count", drpc).stateQuery(callCounts, new Fields("args"), new MapGet(),
				new Fields("count"));

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident", conf, topology.build());

		feederBatchSpout.nextTuple(feederBatchSpout);
		while (true) {
			System.out.println("DRPC : Query starts");
			System.out.println("#####" + drpc.execute("call_count", "7939379393 - 7939379391"));
			System.out.println("DRPC : Query ends");
			Utils.sleep(2000);
		}

	}

}
