package com.edureka.storm.models;

import java.io.Serializable;

public class ProductPriceInventory implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long pogId;

	private String supc;

	private float price;

	private long quantity;

	public Long getPogId() {
		return pogId;
	}

	public void setPogId(Long pogId) {
		this.pogId = pogId;
	}

	public String getSupc() {
		return supc;
	}

	public void setSupc(String supc) {
		this.supc = supc;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public long getQuantity() {
		return quantity;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

}
