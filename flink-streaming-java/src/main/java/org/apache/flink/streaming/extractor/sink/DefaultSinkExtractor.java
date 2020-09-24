package org.apache.flink.streaming.extractor.sink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.extractor.AbstractExtractor;

import java.util.Map;

public class DefaultSinkExtractor extends AbstractExtractor {
	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		return super.sink(jobName, function);
	}
}
