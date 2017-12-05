package com.edureka.storm.spout;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.edureka.storm.helper.ClickStreamParser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ClickFileReaderSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private Scanner scanner;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		openFile(((String) conf.get("clickdatapath")));
	}

	@Override
	public void nextTuple() {
		String value = readLine();

		if (value == null)
			return;

		if (value != null && value.trim().length() > 0) {
			List<String> values = ClickStreamParser.parse(value);
			if (values != null && values.size() > 0) {
				collector.emit(new Values(values.get(0), values.get(1), values.get(2)));
			}
		}
	}

	protected String readLine() {
		if (scanner.hasNextLine())
			return scanner.nextLine();
		else
			return null;
	}

	protected void openFile(String filePath) {
		File file = new File(filePath);
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			System.out.println("Exception occured while opening file = " + e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip", "url", "clientkey"));
	}
}
