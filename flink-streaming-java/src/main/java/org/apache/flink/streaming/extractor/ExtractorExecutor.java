package org.apache.flink.streaming.extractor;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.extractor.sink.*;
import org.apache.flink.streaming.extractor.source.DefaultSourceExtractor;
import org.apache.flink.streaming.extractor.source.FlinkKafkaConsumerExtractor;
import org.apache.flink.streaming.extractor.type.DefaultTypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ExtractorExecutor {
	private static final Logger logger = LoggerFactory.getLogger(ExtractorExecutor.class);

	public void extractSourceOrSink(StreamGraph streamGraph) {
		String jobName = "";
		try {
			if (streamGraph == null) {
				logger.warn("extractSourceOrSink warn: streamGraph is null, jobName={}", jobName);
				return;
			}
			jobName = streamGraph.getJobName();

			for (StreamNode streamNode : streamGraph.getStreamNodes()) {
				IExtraction extraction = getIExtraction(jobName, streamNode);
				extraction.type(jobName, streamNode);
			}

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
			logger.info("extraction ids: jobName={}, source={}, ids={}", jobName, source, ids.toString());
			for (Integer id : ids) {
				StreamNode streamNode = streamGraph.getStreamNode(id);
				SimpleUdfStreamOperatorFactory simpleUdfStreamOperatorFactory = (SimpleUdfStreamOperatorFactory) streamNode.getOperatorFactory();
				if (simpleUdfStreamOperatorFactory == null) {
					logger.warn("extraction warn simpleUdfStreamOperatorFactory is null: jobName={}, source={}, id={}", jobName, source, id);
					continue;
				}
				String functionClassName = simpleUdfStreamOperatorFactory.getUserFunctionClassName();
				IExtraction extraction = getIExtraction(source, functionClassName, jobName);
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

	private IExtraction getIExtraction(String jobName, StreamNode streamNode) {
		return new DefaultTypeExtractor();
	}

	private IExtraction getIExtraction(boolean source, String functionClassName, String jobName) {
		if (functionClassName == null) {
			logger.warn("functionClassName is null, jobName={}", jobName);
			return null;
		}
		switch (functionClassName) {
			case "org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink":
				return new StreamingFileSinkExtractor();
			case "org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer":
				return new FlinkKafkaConsumerExtractor();
			case "org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer":
				return new FlinkKafkaProducerExtractor();
			case "org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink":
				return new FlinkElasticsearch5SinkExtractor();
			case "org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink":
			case "org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink":
				return new FlinkElasticsearchSinkExtractor();
			default:
				if (source) {
					return new DefaultSourceExtractor();
				}
				return new DefaultSinkExtractor();
		}
	}
}
