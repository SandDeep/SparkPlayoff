package com.poc.kafka;

import java.util.Map;

import kafka.common.TopicAndPartition;

public interface DirectStreamChain {

	public void setNext(DirectStreamChain next);
	public void process(Map<TopicAndPartition, Long> map);
}
