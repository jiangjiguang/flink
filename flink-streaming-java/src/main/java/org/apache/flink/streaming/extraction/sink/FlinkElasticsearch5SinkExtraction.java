package org.apache.flink.streaming.extraction.sink;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.extraction.AbstractExtraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkElasticsearch5SinkExtraction extends AbstractExtraction {
	private static final Logger logger = LoggerFactory.getLogger(FlinkElasticsearch5SinkExtraction.class);

	private static final Set<String> fieldSet = Stream.of("callBridge")
		.collect(Collectors.toCollection(HashSet::new));

	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		Map<String, Object> resultMap = new HashMap<>();
		try {
			Class clazz = function.getClass().getSuperclass();
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
				Object value = field.get(function);
				if (value == null) {
					continue;
				}
				String fieldName = field.getName();
				if (!fieldSet.contains(fieldName)) {
					continue;
				}
				if (fieldName.equals("callBridge")) {
					Class callBridgeClazz = value.getClass();
					Field httpHostField = callBridgeClazz.getDeclaredField("transportAddresses");
					Object callBridgeValue = httpHostField.get(value);
					if (callBridgeValue == null) {
						continue;
					}
					resultMap.put("callBridge.transportAddresses", callBridgeValue);
				}
			}
		} catch (Exception ex) {
			logger.error("sink error: jobName={}, exception", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}
}
