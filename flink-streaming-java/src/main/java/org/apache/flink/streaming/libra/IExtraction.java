package org.apache.flink.streaming.libra;

import org.apache.flink.api.common.functions.Function;

import java.util.Map;

public interface IExtraction {
	Map<String, Object> source(String jobName, Function function);

	Map<String, Object> sink(String jobName, Function function);

}
