package org.apache.flink.streaming.libra;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractExtraction implements IExtraction {
	private static final Logger logger = LoggerFactory.getLogger(AbstractExtraction.class);

	@Override
	public Map<String, Object> source(String jobName, Function function) {
		return extractSourceOrSink(jobName, function, null, true);
	}

	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		return extractSourceOrSink(jobName, function, null, true);
	}

	protected Map<String, Object> extractSourceOrSink(String jobName, Function function, Set<String> fieldSet, boolean parent) {
		Map<String, Object> resultMap = new HashMap<>();
		try {
			boolean allFlag = CollectionUtils.isEmpty(fieldSet) ? true : false;

			Class klass = function.getClass();
			Field[] fields = klass.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
				Object value = field.get(function);
				if (value != null && (allFlag || fieldSet.contains(field.getName()))) {
					resultMap.put(field.getName(), value);
				}
			}
			if (parent) {
				Class parentClass = function.getClass().getSuperclass();
				Field[] parentFields = parentClass.getDeclaredFields();
				for (Field field : parentFields) {
					field.setAccessible(true);
					Object value = field.get(function);
					if (value != null && (allFlag || fieldSet.contains(field.getName()))) {
						resultMap.put(field.getName(), value);
					}
				}
			}
		} catch (Exception ex) {
			logger.error("extractSourceOrSink error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}
}
