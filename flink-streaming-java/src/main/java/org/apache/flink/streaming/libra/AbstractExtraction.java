package org.apache.flink.streaming.libra;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.util.MyJSONMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractExtraction implements IExtraction {
	private static final Logger logger = LoggerFactory.getLogger(AbstractExtraction.class);


	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return null;
	}

	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		return null;
	}
}
