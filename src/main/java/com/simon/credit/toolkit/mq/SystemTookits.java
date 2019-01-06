package com.simon.credit.toolkit.mq;

import org.apache.commons.lang3.StringUtils;

public class SystemTookits {

	public static final String getOperatingSystemName() {
		return System.getProperty("os.name");
	}

	public static final boolean isLinuxSystem() {
		return StringUtils.containsIgnoreCase(getOperatingSystemName(), "Linux");
	}

}
