package com.edureka.storm.helper;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoIP2Location {
	private static final Logger LOG = LoggerFactory.getLogger(GeoIP2Location.class);

	private DatabaseReader reader;

	public GeoIP2Location(String dbPath) {
		try {
			File database = new File(dbPath);
			reader = new DatabaseReader.Builder(database).build();
		} catch (IOException ex) {
			LOG.error("Unable to load MaxMind database", ex);
			throw new RuntimeException("Unable to load MaxMind database");
		}
	}

	public Location resolve(String ip) {
		try {
			CityResponse response = reader.city(InetAddress.getByName(ip));

			Location location = new Location();
			location.setCity(response.getCity().getName());
			location.setCountryName(response.getCountry().getName());
			location.setCountryCode(response.getCountry().getIsoCode());
			location.setIp(ip);

			return location;
		} catch (IOException | GeoIp2Exception ex) {
			LOG.debug("Unable to resolve ip location", ex);
		}

		return null;
	}

	public static class Location {
		private String countryName;
		private String countryCode;
		private String city;
		private String ip;

		public String getCountryName() {
			return countryName;
		}

		public void setCountryName(String countryName) {
			this.countryName = countryName;
		}

		public String getCountryCode() {
			return countryCode;
		}

		public void setCountryCode(String countryCode) {
			this.countryCode = countryCode;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

	}

}
