package com.edureka.storm.order.parser;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StreamValues extends Values {

	private static final long serialVersionUID = 1L;

	private Object messageId;
	private String streamId = Utils.DEFAULT_STREAM_ID;

	public StreamValues() {
	}

	public StreamValues(Object... vals) {
		super(vals);
	}

	public Object getMessageId() {
		return messageId;
	}

	public void setMessageId(Object messageId) {
		this.messageId = messageId;
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

}
