package com.iot.streaming;

import java.io.Serializable;

public class MetadataPOJO implements Serializable  {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String deviceguid;
	private int deviceid;
	private String connectorguid;
	private int connectorid;
	private String variablename;
	private int variableid;
	private String variableDataType;
	
	public String getDeviceguid() {
		return deviceguid;
	}
	public void setDeviceguid(String deviceguid) {
		this.deviceguid = deviceguid;
	}
	public int getDeviceid() {
		return deviceid;
	}
	public void setDeviceid(int deviceid) {
		this.deviceid = deviceid;
	}
	public String getConnectorguid() {
		return connectorguid;
	}
	public void setConnectorguid(String connectorguid) {
		this.connectorguid = connectorguid;
	}
	public int getConnectorid() {
		return connectorid;
	}
	public void setConnectorid(int connectorid) {
		this.connectorid = connectorid;
	}
	public String getVariablename() {
		return variablename;
	}
	public void setVariablename(String variablename) {
		this.variablename = variablename;
	}
	public int getVariableid() {
		return variableid;
	}
	public void setVariableid(int variableid) {
		this.variableid = variableid;
	}
	public String getVariableDataType() {
		return variableDataType;
	}
	public void setVariableDataType(String variableDataType) {
		this.variableDataType = variableDataType;
	}
	
}
