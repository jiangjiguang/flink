package org.apache.flink.streaming.libra;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.libra.sink.FlinkKafkaProducerExtraction;
import org.apache.flink.streaming.libra.source.FlinkKafkaConsumerExtraction;
import org.apache.flink.streaming.util.MyJSONMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ExtractionExecutor {
	private static final Logger logger = LoggerFactory.getLogger(ExtractionExecutor.class);

	public void getSourceOrSink(StreamGraph streamGraph) {
		MyJSONMapper myJSONMapper = new MyJSONMapper();
		String jobName = "";
		try {
			if (streamGraph == null) {
				logger.warn("extraction warn, streamGraph is null, jobName={}", streamGraph.getJobName());
				return;
			}
			jobName = streamGraph.getJobName();
			Collection<Integer> sourceIds = streamGraph.getSourceIDs();
			if (CollectionUtils.isEmpty(sourceIds)) {
				logger.warn("extraction warn, sourceIds is empty, jobName={}", streamGraph.getJobName());
			}

			Collection<Integer> sinkIds = streamGraph.getSinkIDs();
			if (CollectionUtils.isEmpty(sourceIds)) {
				logger.warn("extraction warn, sinkIds is empty, jobName={}", streamGraph.getJobName());
			}
			logger.info("extraction sourceIds: sourceIds={}", myJSONMapper.toJSONString(sourceIds));
			logger.info("extraction sinkIds: sinkIds={}", myJSONMapper.toJSONString(sinkIds));
			if (CollectionUtils.isNotEmpty(sourceIds)) {
				for (Integer sourceId : sourceIds) {
					StreamNode streamNode = streamGraph.getStreamNode(sourceId);
					SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
					if (simpleUdfStreamOperatorFactory == null) {
						logger.warn("extraction warn simpleUdfStreamOperatorFactory is null, jobName={}", streamGraph.getJobName());
						continue;
					}
					logger.info("source function className: jobName={}, className={}", streamGraph.getJobName(), simpleUdfStreamOperatorFactory.getUserFunctionClassName());
					IExtraction extraction = getIExtraction(simpleUdfStreamOperatorFactory.getUserFunctionClassName(), streamGraph.getJobName());
					if (extraction == null) {
						logger.warn("extraction source warn extraction is null: jobName={}, className={}", streamGraph.getJobName(), simpleUdfStreamOperatorFactory.getUserFunctionClassName());
						continue;
					}
					Map<String, Object> sourceResultMap = extraction.source(jobName, simpleUdfStreamOperatorFactory.getUserFunction());
					logger.info("taskSources result: jobName={}, data={}", jobName, myJSONMapper.toJSONString(sourceResultMap));
				}
			}

			if (CollectionUtils.isNotEmpty(sinkIds)) {
				for (Integer sinkId : sinkIds) {
					StreamNode streamNode = streamGraph.getStreamNode(sinkId);
					SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
					if (simpleUdfStreamOperatorFactory == null) {
						logger.warn("simpleUdfStreamOperatorFactory is null, jobName={}", streamGraph.getJobName());
						continue;
					}
					logger.info("sink function className: jobName={}, className={}", streamGraph.getJobName(), simpleUdfStreamOperatorFactory.getUserFunctionClassName());
					IExtraction extraction = getIExtraction(simpleUdfStreamOperatorFactory.getUserFunctionClassName(), streamGraph.getJobName());
					if (extraction == null) {
						logger.warn("extraction sink warn extraction is null: jobName={}, className={}", streamGraph.getJobName(), simpleUdfStreamOperatorFactory.getUserFunctionClassName());
						continue;
					}

					Map<String, Object> sinkResultMap = extraction.sink(jobName, simpleUdfStreamOperatorFactory.getUserFunction());
					logger.info("taskSinks result: jobName={}, data={}", jobName, myJSONMapper.toJSONString(sinkResultMap));
				}
			}
		} catch (Exception ex) {
			logger.error("getSourceOrSink error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
	}

	private IExtraction getIExtraction(String functionClassName, String jobName) {
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
				logger.warn("getIExtraction warn not: functionClassName={}, jobName={}", functionClassName, jobName);
				return null;
		}
	}
}
