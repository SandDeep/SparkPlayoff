package com.poc.spark;

import java.io.Serializable;

public class DStreamWrapper<T> implements Serializable {

	private static final long serialVersionUID = 1530500622120664331L;

	final T dstreamobj;

	public DStreamWrapper(T t) {
		if (t == null) {
			throw new NullPointerException();
		}
		this.dstreamobj = t;
	}

	public T getDstreamobj() {
		return dstreamobj;
	}

	
}
