package org.apache.flink.client.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class JavaEnvUtils {
	private static final Logger LOG = LoggerFactory.getLogger(JavaEnvUtils.class);

	public static String getVirtualMachineName() {
		try {
			RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
			return runtimeMXBean.getName();
		} catch (Exception ex) {
			LOG.error("getVirtualMachineName error: exception={}", ExceptionUtils.getStackTrace(ex));
			return null;
		}
	}
}
