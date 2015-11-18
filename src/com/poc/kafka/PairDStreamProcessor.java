package com.poc.kafka;

import java.util.Map;

import kafka.common.TopicAndPartition;

public class PairDStreamProcessor implements DirectStreamChain {

	private DirectStreamChain nextChain;
	
	public void setNext(DirectStreamChain next) {
		this.nextChain=next;
	}

	public void process(Map<TopicAndPartition, Long> map) {

	}

}
