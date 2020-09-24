package org.apache.flink.streaming.extractor.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.extractor.AbstractExtractor;

import java.util.Map;

public class DefaultSourceExtractor extends AbstractExtractor {
	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return super.source(jobName, function);
	}
}
