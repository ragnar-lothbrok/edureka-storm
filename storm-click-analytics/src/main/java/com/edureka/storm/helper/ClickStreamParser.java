package com.edureka.storm.helper;

import com.edureka.storm.dtos.ClickStream;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickStreamParser {

	private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

	private final static Gson gson = new Gson();

	public static List<String> parse(String input) {
		try {
			ClickStream clickstream = gson.fromJson(input, ClickStream.class);
			return ImmutableList.of(clickstream.getIp(), clickstream.getUrl(), clickstream.getClientKey());
		} catch (JsonSyntaxException ex) {
			LOG.error("Error parsing JSON encoded clickstream: " + input, ex);
		}
		return ImmutableList.of();
	}

}
