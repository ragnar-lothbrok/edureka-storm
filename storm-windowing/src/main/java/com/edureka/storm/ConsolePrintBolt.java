package com.edureka.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class ConsolePrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private TopologyContext context;

	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println(
				format(context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId()), tuple));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public static String format(Fields schema, Tuple tuple) {
		String line = "";
		for (int i = 0; i < tuple.size(); i++) {
			if (i != 0)
				line += ", ";
			line += String.format("%s=%s", schema.get(i), tuple.getValue(i));
		}
		return line;
	}

}
