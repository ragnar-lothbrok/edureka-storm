package com.edureka.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CSVFileSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(BaseRichSpout.class);
	private SpoutOutputCollector spoutOutputCollector;
	private String filePath;
	private LinkedBlockingQueue<String> queue;

	public CSVFileSpout(String filePath) {
		this.filePath = filePath;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<String>(Integer.MAX_VALUE);
		try {
			this.spoutOutputCollector = collector;
			CsvReader csvReader = new CsvReader(filePath);
			csvReader.readHeaders();
			while (csvReader.readRecord()) {
				queue.offer(csvReader.get("keyword"));
			}
		} catch (Exception exception) {
			logger.error("Exception occured " + exception.getMessage());
		}
	}

	public void nextTuple() {
		String keyword = queue.poll();
		if (keyword == null) {
			Utils.sleep(50);
		} else {
			spoutOutputCollector.emit(new Values(keyword));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("keyword"));
	}

}
