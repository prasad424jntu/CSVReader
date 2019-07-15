package com.product.productcsvreader;

import java.io.Serializable;

public class Product implements Serializable{
	
	String uuId;
	String name;
	String description;
	String provider;
	String available;
	String measurementUnits;
	public String getUuId() {
		return uuId;
	}
	public void setUuId(String uuId) {
		this.uuId = uuId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getProvider() {
		return provider;
	}
	public void setProvider(String provider) {
		this.provider = provider;
	}
	public String getMeasurementUnits() {
		return measurementUnits;
	}
	public void setMeasurementUnits(String measurementUnits) {
		this.measurementUnits = measurementUnits;
	}
	public String getAvailable() {
		return available;
	}
	public void setAvailable(String available) {
		this.available = available;
	}
	
	@Override
	public String toString() {
		return "[uuid="+this.getUuId() + ",name" + this.getName() + ",description" + this.getDescription() + 
				",provider" + this.getProvider()
		         + ",measurementunits" +this.getMeasurementUnits() +
		         ",available"+ this.getAvailable() +"]";
	}
	
	
}
