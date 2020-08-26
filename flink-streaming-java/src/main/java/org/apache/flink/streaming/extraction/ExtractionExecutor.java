package org.apache.flink.streaming.extraction;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.extraction.sink.DefaultSinkExtraction;
import org.apache.flink.streaming.extraction.sink.FlinkKafkaProducerExtraction;
import org.apache.flink.streaming.extraction.source.DefaultSourceExtraction;
import org.apache.flink.streaming.extraction.source.FlinkKafkaConsumerExtraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ExtractionExecutor {
	private static final Logger logger = LoggerFactory.getLogger(ExtractionExecutor.class);

	public void extractSourceOrSink(StreamGraph streamGraph) {
		String jobName = "";
		try {
			if (streamGraph == null) {
				logger.warn("extractSourceOrSink warn: streamGraph is null, jobName={}", jobName);
				return;
			}
			jobName = streamGraph.getJobName();
			Collection<Integer> sourceIds = streamGraph.getSourceIDs();
			Collection<Integer> sinkIds = streamGraph.getSinkIDs();

			extraction(sourceIds, streamGraph, jobName, true);
			extraction(sinkIds, streamGraph, jobName, false);
		} catch (Exception ex) {
			logger.error("extractSourceOrSink error: jobName={}, exception={}", jobName, ExceptionUtils.getStackTrace(ex));
		}
	}

	private void extraction(Collection<Integer> ids, StreamGraph streamGraph, String jobName, boolean source) {
		try {
			if (CollectionUtils.isEmpty(ids)) {
				return;
			}
			for (Integer id : ids) {
				StreamNode streamNode = streamGraph.getStreamNode(id);
				SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
				if (simpleUdfStreamOperatorFactory == null) {
					logger.warn("extraction warn simpleUdfStreamOperatorFactory is null: jobName={}, source={}, id={}", jobName, source, id);
					continue;
				}
				String functionClassName = simpleUdfStreamOperatorFactory.getUserFunctionClassName();
				IExtraction extraction = getIExtraction(true, functionClassName, jobName);
				if (extraction == null) {
					logger.warn("extraction warn IExtraction is null: jobName={}, source={}, id={}, className={}", jobName, source, id, functionClassName);
					continue;
				}
				Map<String, Object> sourceResultMap = extraction.sink(jobName, simpleUdfStreamOperatorFactory.getUserFunction());
				logger.info("extraction result: jobName={}, source={}, functionClassName={}, data={}", jobName, source, functionClassName, sourceResultMap.toString());
			}
		} catch (Exception ex) {
			logger.error("extraction item: jobName={}, source={}, ids={}", jobName, source, ids.toString());
			return;
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
