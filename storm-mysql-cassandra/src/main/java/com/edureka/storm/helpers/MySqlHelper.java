package com.edureka.storm.helpers;

import java.io.Serializable;
import java.sql.PreparedStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.storm.models.Product;

import backtype.storm.tuple.Tuple;

public class MySqlHelper implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MySqlHelper.class);

	private static final long serialVersionUID = 1L;
	private static MySqlConnection conn;

	public MySqlHelper(String ip, String database, String username, String password) {
		conn = new MySqlConnection(ip, database, username, password);
		conn.open();
	}

	public void persist(Tuple tuple) {
		Product product = ((Product) tuple.getValue(0));
		PreparedStatement statement = null;
		try {
			statement = conn.getConnection()
					.prepareStatement("insert into product_inventory (pogid, supc,price,quantity) values (?, ?,?,?)"
							+ " on duplicate key update price =? , quantity = ?");
			statement.setLong(1, product.getPogId());
			statement.setString(2, product.getSupc());
			statement.setFloat(3, product.getPrice());
			statement.setLong(4, product.getQuantity());
			statement.setFloat(5, product.getPrice());
			statement.setLong(6, product.getQuantity());
			statement.executeUpdate();
		} catch (Exception ex) {
			LOG.error("Exception occured while pushing data to sql = {}", ex);
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					LOG.error("Exception occured while closing statement = {}", ex);
				}
			}
		}
	}

	public void close() {
		conn.close();
	}
}
