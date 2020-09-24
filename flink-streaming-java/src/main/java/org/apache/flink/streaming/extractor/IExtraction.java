package org.apache.flink.streaming.extractor;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Map;

public interface IExtraction {
	Map<String, Object> source(String jobName, Function function);

	Map<String, Object> sink(String jobName, Function function);

	Map<String, Object> type(String jobName, StreamNode streamNode);
}
