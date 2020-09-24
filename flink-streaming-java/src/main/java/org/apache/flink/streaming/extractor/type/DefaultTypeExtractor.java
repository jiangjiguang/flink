package org.apache.flink.streaming.extractor.type;

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.extractor.AbstractExtractor;

import java.util.Map;

public class DefaultTypeExtractor extends AbstractExtractor {


	@Override
	public Map<String, Object> type(String jobName, StreamNode streamNode) {
		return super.type(jobName, streamNode);
	}
}
