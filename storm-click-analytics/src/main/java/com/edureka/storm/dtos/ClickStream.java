package com.edureka.storm.dtos;

import java.io.Serializable;

public class ClickStream implements Serializable {

	private static final long serialVersionUID = 1L;

	private String url;

	private String ip;

	private String clientKey;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getClientKey() {
		return clientKey;
	}

	public void setClientKey(String clientKey) {
		this.clientKey = clientKey;
	}

}
