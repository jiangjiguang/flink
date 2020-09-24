package org.apache.flink.streaming.extractor;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractExtractor implements IExtraction {
	private static final Logger logger = LoggerFactory.getLogger(AbstractExtractor.class);

	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return extractSourceOrSink(jobName, function, null, true);
	}

	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		return extractSourceOrSink(jobName, function, null, true);
	}

	@Override
	public Map<String, Object> type(String jobName, StreamNode streamNode) {
		Map<String, Object> resultMap = new HashMap<>();
		try {
			Class clazz = streamNode.getClass();
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
				Object value = field.get(streamNode);
				logger.info("extractType field={}, value={}", field.getName(), value);

				if (value == null) {
					continue;
				}
			}
		} catch (Exception ex) {
			logger.error("extractType error: {}, {}", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}

	protected Map<String, Object> extractSourceOrSink(String jobName, Function function, Set<String> fieldSet, boolean parent) {
		Map<String, Object> resultMap = new HashMap<>();
		try {
			resultMap.putAll(extractItem(false, function, fieldSet));
			if (parent) {
				extractItem(true, function, fieldSet);
			}
		} catch (Exception ex) {
			logger.error("extractSourceOrSink error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}


	private Map<String, Object> extractItem(boolean parent, Function function, Set<String> fieldSet) throws Exception {
		Map<String, Object> resultMap = new HashMap<>();

		Class clazz = function.getClass();
		if (parent) {
			clazz = function.getClass().getSuperclass();
		}

		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			Object value = field.get(function);
			if (value == null) {
				continue;
			}
			if (CollectionUtils.isEmpty(fieldSet) || fieldSet.contains(field.getName())) {
				resultMap.put(field.getName(), value);
			}
		}
		return resultMap;
	}
}
