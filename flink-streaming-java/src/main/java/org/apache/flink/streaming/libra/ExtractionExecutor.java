package org.apache.flink.streaming.libra;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.libra.sink.DefaultSinkExtraction;
import org.apache.flink.streaming.libra.sink.FlinkKafkaProducerExtraction;
import org.apache.flink.streaming.libra.source.DefaultSourceExtraction;
import org.apache.flink.streaming.libra.source.FlinkKafkaConsumerExtraction;
import org.apache.flink.streaming.util.MyJSONMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ExtractionExecutor {
	private static final Logger logger = LoggerFactory.getLogger(ExtractionExecutor.class);

	public void extractSourceOrSink(StreamGraph streamGraph) {
		String jobName = "";
		try {
			MyJSONMapper myJSONMapper = new MyJSONMapper();
			if (streamGraph == null) {
				logger.warn("extractSourceOrSink warn: streamGraph is null, jobName={}", jobName);
				return;
			}
			jobName = streamGraph.getJobName();
			Collection<Integer> sourceIds = streamGraph.getSourceIDs();
			if (CollectionUtils.isEmpty(sourceIds)) {
				logger.warn("extractSourceOrSink warn: sourceIds is empty, jobName={}", jobName);
			}
			Collection<Integer> sinkIds = streamGraph.getSinkIDs();
			logger.info("extractSourceOrSink sourceIds:  jobName={}, sourceIds={}", jobName, myJSONMapper.toJSONString(sourceIds));
			logger.info("extractSourceOrSink sinkIds:  jobName={}, sinkIds={}", jobName, myJSONMapper.toJSONString(sinkIds));
			if (CollectionUtils.isNotEmpty(sourceIds)) {
				for (Integer sourceId : sourceIds) {
					StreamNode streamNode = streamGraph.getStreamNode(sourceId);
					SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
					if (simpleUdfStreamOperatorFactory == null) {
						logger.warn("extraction source warn simpleUdfStreamOperatorFactory is null: jobName={}", jobName);
						continue;
					}
					String functionClassName = simpleUdfStreamOperatorFactory.getUserFunctionClassName();
					IExtraction extraction = getIExtraction(true, functionClassName, jobName);
					if (extraction == null) {
						logger.warn("extraction source warn extraction is null: jobName={}, className={}", jobName, functionClassName);
						continue;
					}
					Map<String, Object> sourceResultMap = extraction.source(jobName, simpleUdfStreamOperatorFactory.getUserFunction());
					logger.info("extraction source result: jobName={}, functionClassName={}, data={}", jobName, functionClassName, myJSONMapper.toJSONString(sourceResultMap));
				}
			}

			if (CollectionUtils.isNotEmpty(sinkIds)) {
				for (Integer sinkId : sinkIds) {
					StreamNode streamNode = streamGraph.getStreamNode(sinkId);
					SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
					if (simpleUdfStreamOperatorFactory == null) {
						logger.warn("extraction sink warn simpleUdfStreamOperatorFactory is null: jobName={}", jobName);
						continue;
					}
					String functionClassName = simpleUdfStreamOperatorFactory.getUserFunctionClassName();
					IExtraction extraction = getIExtraction(true, functionClassName, jobName);
					if (extraction == null) {
						logger.warn("extraction sink warn extraction is null: jobName={}, className={}", jobName, functionClassName);
						continue;
					}
					Map<String, Object> sourceResultMap = extraction.sink(jobName, simpleUdfStreamOperatorFactory.getUserFunction());
					logger.info("extraction sink result: jobName={}, functionClassName={}, data={}", jobName, functionClassName, myJSONMapper.toJSONString(sourceResultMap));
				}
			}
		} catch (Exception ex) {
			logger.error("extractSourceOrSink error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
	}

	private IExtraction getIExtraction(boolean source, String functionClassName, String jobName) {
		if (functionClassName == null) {
			logger.warn("functionClassName is null, jobName={}", jobName);
			return null;
		}
		switch (functionClassName) {
			case "org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer":
				return new FlinkKafkaConsumerExtraction();
			case "org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer":
				return new FlinkKafkaProducerExtraction();
			default:
				if (source) {
					return new DefaultSourceExtraction();
				}
				return new DefaultSinkExtraction();
		}
	}
}
