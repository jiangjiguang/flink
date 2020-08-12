package org.apache.flink.streaming.libra.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.libra.AbstractExtraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkKafkaConsumerExtraction extends AbstractExtraction {
	private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaConsumerExtraction.class);

	private static final Set<String> fields = Stream.of("properties",
		"topicsDescriptor", "discoveryIntervalMillis", "startupMode", "KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS")
		.collect(Collectors.toCollection(HashSet::new));

	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return extractSourceOrSink(jobName, function, fields, true);
	}
}
