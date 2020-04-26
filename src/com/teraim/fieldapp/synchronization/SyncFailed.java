package com.teraim.fieldapp.synchronization;

import java.io.Serializable;

public class SyncFailed implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9068482556518812383L;
	private String reason;
	public SyncFailed(String reason) {
		this.reason=reason;
	}
	public String getReason() {
		return reason;
	}
}
