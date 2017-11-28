package com.edureka.storm.bolts;

import java.io.Serializable;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BasicFormatter implements Serializable {

	private static final long serialVersionUID = 1L;

	public static String format(Fields schema, Tuple tuple) {
		String line = "";
		for (int i = 0; i < tuple.size(); i++) {
			if (i != 0)
				line += ", ";
			line += String.format("%s=%s", schema.get(i), tuple.getValue(i));
		}
		return line;
	}

	public BasicFormatter() {

	}

}
