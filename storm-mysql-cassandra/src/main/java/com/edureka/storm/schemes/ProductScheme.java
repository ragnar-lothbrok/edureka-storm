package com.edureka.storm.schemes;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.storm.models.Product;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class ProductScheme implements Scheme {

	private static final Logger LOG = LoggerFactory.getLogger(ProductScheme.class);

	private static final long serialVersionUID = 1L;

	private static final Gson GSON = new Gson();

	@Override
	public List<Object> deserialize(byte[] ser) {
		Product product = null;
		List<Object> events = Lists.newArrayListWithCapacity(1);
		try {
			product = GSON.fromJson(new String(ser, "UTF-8"), Product.class);
			events.add(product);
			LOG.debug("Event: {}  ", product);
		} catch (Exception e) {
			LOG.error("Cannot convert record due to= {} ", ExceptionUtils.getStackTrace(e));
		}
		return events;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("product");
	}

}
