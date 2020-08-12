package org.apache.flink.streaming.libra.sink;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.libra.AbstractExtraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class FlinkKafkaProducerExtraction extends AbstractExtraction {
	private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaProducerExtraction.class);


	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		logger.info("sink param: jobName={}, function={}", jobName, function.getClass().getName());
		Map<String, Object> resultMap = new HashMap<>();
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
		} catch (Exception ex) {
			logger.error("sink extraction error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}
}
