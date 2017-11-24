package com.edureka.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class WordCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;

	private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);

	/**
	 * After this interval top list will be printed
	 */
	private final long logIntervalInSeconds;

	/**
	 * After this interval top list will be cleared.
	 */
	private final long clearIntervalInSeconds;

	/**
	 * Number of top words to be printed
	 */
	private final int topWordListSize;

	private Map<String, Long> counter;
	private long lastLogTime;
	private long lastClearTime;

	public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
		this.logIntervalInSeconds = logIntervalSec;
		this.clearIntervalInSeconds = clearIntervalSec;
		this.topWordListSize = topListSize;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		counter = new HashMap<String, Long>();
		lastLogTime = System.currentTimeMillis();
		lastClearTime = System.currentTimeMillis();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	@Override
	public void execute(Tuple input) {
		String word = (String) input.getValueByField("word");
		counter.put(word, (counter.get(word) == null ? 1L : counter.get(word) + 1));

		long now = System.currentTimeMillis();
		long logPeriodSec = (now - lastLogTime) / 1000;
		if (logPeriodSec > logIntervalInSeconds) {
			logger.info("\nWord count: " + counter.size());
			publishTopList();
			lastLogTime = now;
		}
	}

	private void publishTopList() {
		// calculate top list:
		SortedMap<Long, String> top = new TreeMap<Long, String>();
		for (Map.Entry<String, Long> entry : counter.entrySet()) {
			long count = entry.getValue();
			String word = entry.getKey();

			top.put(count, word);
			// removing earliest entries which have less count
			if (top.size() > topWordListSize) {
				top.remove(top.firstKey());
			}
		}

		top.entrySet().forEach(e -> System.out.println("top - " + e.getValue() + "|" + e.getKey()));

		// Clear top list
		long now = System.currentTimeMillis();
		if (now - lastClearTime > clearIntervalInSeconds * 1000) {
			counter.clear();
			lastClearTime = now;
		}
	}
}
