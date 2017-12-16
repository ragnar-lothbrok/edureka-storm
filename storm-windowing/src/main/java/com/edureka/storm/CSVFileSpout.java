package com.edureka.storm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;

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