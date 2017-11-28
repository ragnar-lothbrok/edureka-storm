package com.edureka.storm.topology;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class FormatCallDuration extends BaseFunction {

	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String fromMobileNumber = tuple.getString(0);
		String toMobileNumber = tuple.getString(1);
		collector.emit(new Values(fromMobileNumber + " - " + toMobileNumber, tuple.getInteger(2)));
	}
}