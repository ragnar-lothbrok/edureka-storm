package com.edureka.storm.order.spouts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import com.edureka.storm.order.parser.CommonLogParser;
import com.edureka.storm.order.parser.StreamValues;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class FileSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private String filePath;
	private Scanner scanner;
	private CommonLogParser commonLogParser;

	public FileSpout(final String filePath, final CommonLogParser commonLogParser) {
		this.filePath = filePath;
		this.commonLogParser = commonLogParser;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		openFile();
	}

	@Override
	public void nextTuple() {
		String value = readLine();

		if (value == null)
			return;

		List<StreamValues> tuples = commonLogParser.parse(value);

		if (tuples != null) {
			for (StreamValues values : tuples) {
				String msgId = UUID.randomUUID().toString();
				collector.emit(values.getStreamId(), values, msgId);
			}
		}
	}

	protected String readLine() {
		if (scanner.hasNextLine())
			return scanner.nextLine();
		else
			return null;
	}

	protected void openFile() {
		File file = new File(filePath);
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			System.out.println("Exception occured while opening file = " + e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(CommonLogParser.TX_DATE, CommonLogParser.CITY, CommonLogParser.PAYMENT_TYPE,
				CommonLogParser.STATE));
	}
}
