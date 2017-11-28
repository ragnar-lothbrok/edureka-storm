package com.edureka.storm.spouts;

import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

import com.google.common.collect.ImmutableList;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.testing.FeederBatchSpout;

public class CallLogReaderSpout extends FeederBatchSpout {

	private static final long serialVersionUID = 1L;

	public CallLogReaderSpout(List<String> fields) {
		super(fields);
	}

	private Random randomGenerator = new Random();

	public void nextTuple(final CallLogReaderSpout callLogReaderSpout) {
		new Thread(new Runnable() {

			public void run() {
				while (true) {
					int count = 0;
					while (count < 1000) {
						count++;
						if (count % 2 == 0) {
							String fromMobileNumber = "79393" + RandomStringUtils.randomNumeric(5);
							String toMobileNumber = "79393" + RandomStringUtils.randomNumeric(5);

							if (!fromMobileNumber.equals(toMobileNumber)) {
								Integer duration = randomGenerator.nextInt(60);
								callLogReaderSpout
										.feed(new Values(ImmutableList.of(fromMobileNumber, toMobileNumber, duration)));
							}
						} else {
							String fromMobileNumber = "7939379393";
							String toMobileNumber = "7939379391";

							if (!fromMobileNumber.equals(toMobileNumber)) {
								Integer duration = randomGenerator.nextInt(60);
								callLogReaderSpout
										.feed(new Values(ImmutableList.of(fromMobileNumber, toMobileNumber, duration)));
							}
						}
					}
					Utils.sleep(1000);
				}
			}
		}).start();
		;
	}

}
