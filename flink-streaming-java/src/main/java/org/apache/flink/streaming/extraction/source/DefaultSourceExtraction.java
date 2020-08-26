package org.apache.flink.streaming.extraction.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.extraction.AbstractExtraction;

import java.util.Map;

public class DefaultSourceExtraction extends AbstractExtraction {
	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return super.source(jobName, function);
	}
}
