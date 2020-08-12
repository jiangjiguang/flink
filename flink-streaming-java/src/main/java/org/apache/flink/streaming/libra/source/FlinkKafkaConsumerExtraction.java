package org.apache.flink.streaming.libra.source;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.libra.AbstractExtraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;

public class FlinkKafkaConsumerExtraction extends AbstractExtraction {
	private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaConsumerExtraction.class);

	@Override
	public Map<String, Object> source(String jobName, Function function) {
		Map<String, Object> resultMap = Maps.newHashMap();
		try {
			Class klass = function.getClass();
			Field[] fields = klass.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
				Object value = field.get(function);
				if (value == null) {
					continue;
				}
				resultMap.put(field.getName(), value);
			}
			Class parentClass = function.getClass().getSuperclass();
			Field[] parentFields = parentClass.getDeclaredFields();
			for (Field field : parentFields) {
				field.setAccessible(true);
				Object value = field.get(function);
				if (value == null) {
					continue;
				}
				resultMap.put(field.getName(), value);
			}
		} catch (Exception ex) {
			logger.error("source extraction error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}
}
