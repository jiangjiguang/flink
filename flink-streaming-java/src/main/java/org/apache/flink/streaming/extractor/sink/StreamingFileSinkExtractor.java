package org.apache.flink.streaming.extractor.sink;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.extractor.AbstractExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class StreamingFileSinkExtractor extends AbstractExtractor {
	private static final Logger logger = LoggerFactory.getLogger(StreamingFileSinkExtractor.class);

	@Override
	public Map<String, Object> sink(String jobName, Function function) {
		Map<String, Object> resultMap = new HashMap<>();
		try {
			Class clazz1 = function.getClass();
			Field[] field1s = clazz1.getDeclaredFields();
			for (Field field1 : field1s) {
				field1.setAccessible(true);
				String name1 = field1.getName();
				Object value1 = field1.get(function);
				if (name1 == null || value1 == null) {
					continue;
				}
				if (!name1.equals("bucketsBuilder")) {
					continue;
				}

				Class clazz2 = value1.getClass();
				Field[] field2s = clazz2.getDeclaredFields();
				for (Field field2 : field2s) {
					field2.setAccessible(true);
					String name2 = field2.getName();
					Object value2 = field2.get(value1);
					if (name2 == null || value2 == null) {
						continue;
					}
					if (name2.equals("bucketCheckInterval")) {
						resultMap.put("bucketsBuilder.bucketCheckInterval", value2);
					}

					if (name2.equals("basePath")) {
						resultMap.put("bucketsBuilder.basePath", value2);
					}

					if (name2.equals("bucketAssigner")) {
						Class clazz3 = value2.getClass();
						Field[] field3s = clazz3.getDeclaredFields();
						for (Field field3 : field3s) {
							field3.setAccessible(true);
							String name3 = field3.getName();
							Object value3 = field3.get(value2);
							if (name3 == null || value3 == null) {
								continue;
							}
							if (name3.equals("partitionField")) {
								resultMap.put("bucketsBuilder.bucketAssigner.partitionField", value3);
							}
							if (name3.equals("partitionFormat")) {
								resultMap.put("bucketsBuilder.bucketAssigner.partitionFormat", value3);
							}
							if (name3.equals("timePartitionField")) {
								resultMap.put("bucketsBuilder.bucketAssigner.timePartitionField", value3);
							}
							if (name3.equals("timePartitionFormat")) {
								resultMap.put("bucketsBuilder.bucketAssigner.timePartitionFormat", value3);
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			logger.error("sink error: jobName={}, exception", jobName, ExceptionUtils.getStackTrace(ex));
		}
		return resultMap;
	}
}
