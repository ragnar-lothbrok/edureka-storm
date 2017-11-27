package com.edureka.storm.order.parser;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;

import org.apache.storm.guava.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonLogParser implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);
	private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("MM/dd/yy HH:mm");
	private static final DateTimeFormatter dtFormatter1 = DateTimeFormat.forPattern("dd-MMM-yyyy");

	public static final String TX_DATE = "TX_DATE";
	public static final String CITY = "CITY";
	public static final String STATE = "STATE";
	public static final String PAYMENT_TYPE = "PAYMENT_TYPE";

	public static final String COUNT = "COUNT";

	public List<StreamValues> parse(String str) {

		if (str == null) {
			LOG.warn("Unable to parse log: {}", str);
			return null;
		}
		String split[] = str.split(",");
		String currentDate = null;
		DateTime txDate = null;
		try {
			txDate = dtFormatter.parseDateTime(split[0].trim());
			currentDate = txDate.toString(dtFormatter1);
		} catch (Exception e) {
			LOG.error("Exception occured while parsing date = {} date {} ", e, split[0].trim());
		}

		String city = split[5].trim();

		String paymentType = split[3].trim();
		
		String state = split[6].trim();

		int msgId = String.format("%s:%s", Calendar.getInstance(), currentDate).hashCode();

		StreamValues values = new StreamValues(currentDate, city, paymentType,state);
		values.setMessageId(msgId);

		return ImmutableList.of(values);
	}

}
