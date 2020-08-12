package org.apache.flink.streaming.libra.sink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.libra.AbstractExtraction;

import java.util.Map;

public class DefaultSinkExtraction extends AbstractExtraction {
	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		return super.sink(jobName, function);
	}
}
