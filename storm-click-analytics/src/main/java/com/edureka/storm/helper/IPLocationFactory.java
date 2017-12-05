package com.edureka.storm.helper;

public class IPLocationFactory {

	public static GeoIP2Location create(String name) {
		return new GeoIP2Location(name);
	}
}
