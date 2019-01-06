package com.simon.credit.toolkit.mq;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simon.credit.toolkit.common.CommonToolkits;
import com.simon.credit.toolkit.io.ConsoleToolkits;
import com.simon.credit.toolkit.io.IOToolkits;

/**
 * MQ重发
 * @author XUZIMING 2017-12-27
 */
public class MqReissue {
	private static final Logger LOGGER = LoggerFactory.getLogger(MqReissue.class);

	private static String logFile;
	private static String mqKeyword;
	private static String egrep;
	private static String mqTopic;

	public static void main(String[] args) {
		while (true) {
			String tips = "-----------------------\n"
						+ "please select function:\n"
						+ "-----------------------\n"
						+ "0: exit\n" 
						+ "1: settings \n"
						+ "2: mq analyze\n" 
						+ "3: mq reissue\n"
						+ "4: reissue with file\n";

			String func = ConsoleToolkits.readString(tips);

			boolean isNumeric = isNumeric(func);
			if (!isNumeric) {
				continue;
			}

			int function = Integer.parseInt(func);
			switch (function) {
				case 0 : exit();	 		return;
				case 1 : settings();  		break;
				case 2 : mqReissue(false);  break;
				case 3 : mqReissue(true); 	break;
				case 4 : mqReissueAtFile(); break;
				default: 			  		break;
			}
		}
	}

	/**
	 * 利用正则表达式判断字符串是否是数字
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str) {
		if (str == null || "".equals(str.trim())) {
			return false;
		}

		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);

		return isNum.matches();
	}

	/**
	 * 日志分析设置
	 */
	private static void settings() {
		logFile   = ConsoleToolkits.readString("请输入日志文件路径: \n");
		mqKeyword = ConsoleToolkits.readString("请输入消息关键字: \n");
		egrep 	  = ConsoleToolkits.readString("请输入过滤条件: \n");
	}

	/**
	 * MQ分析与重发
	 * @param needReissue 是否需要重发(true:分析并重发, false:仅分析不重发)
	 */
	private static void mqReissue(boolean needReissue) {
		if (CommonToolkits.isEmpty(logFile) || !new File(logFile).exists()) {
			return;
		}

		List<String> mqMsgBodys = getMQMessageBody(logFile, mqKeyword, egrep);
		LOGGER.info("=== 检索到符合条件的MQ消息数量: {}", mqMsgBodys.size());
		if (CommonToolkits.isEmpty(mqMsgBodys)) {
			return;
		}

		for (String mqMsgBody : mqMsgBodys) {
			if (CommonToolkits.isEmptyContainNull(mqMsgBody)) {
				continue;
			}

			mqMsgBody = CommonToolkits.trimToEmpty(mqMsgBody);
			LOGGER.info("\n" + mqMsgBody + "\n");

			if (needReissue) {
				if (CommonToolkits.isEmptyContainNull(mqTopic)) {
					mqTopic = ConsoleToolkits.readString("请输入MQ消息主题: \n");
				}
				// 重发MQ消息
				MQToolkits.sendMessge(mqTopic, mqMsgBody);
			}
		}
	}

	private static void mqReissueAtFile() {
		String filePath = ConsoleToolkits.readString("请输入要发送的MQ消息所在文件路径: \n");
		// MQ_DM_RM_CREDIT_TOPIC_DECISION_PARAM
		String topic = ConsoleToolkits.readString("请输入MQ消息主题: \n");
		try {
			List<String> lines = IOToolkits.readLines(new File(filePath));

			int count = 0;
			for (String line : lines) {
				if (CommonToolkits.isEmptyContainNull(line)) {
					continue;
				}
				line = StringUtils.replace(line, "\\", "");
				line = StringUtils.replace(line, "null", "\"\"");
				line = StringUtils.replace(line, "\"{", "{");
				line = StringUtils.replace(line, "}\"", "}");

				try {
					Thread.sleep(2000);
				} catch (Exception e) {
					// ignore
				}

				MQToolkits.sendMessge(topic, line);
				LOGGER.info(line);
				count++;
			}

			LOGGER.info("=== total msg :" + count);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	/**
	 * 从日志文件中解析指定的MQ消息体
	 * @param logFile 日志文件路径
	 * @param mqKeyword MQ日志关键字
	 * @param egrep 二次过滤条件
	 * @return
	 */
	private static List<String> getMQMessageBody(String logFile, String mqKeyword, String egrep) {
		Runtime runtime = Runtime.getRuntime();
		logFile = CommonToolkits.castToJavaFilePath(logFile);

		String command = "cat " + logFile + " |grep '" + mqKeyword + "' | egrep '" + egrep + "'|grep -o '{.*}'";
		String[] cmdarray = { "/bin/sh", "-c", command };

		try {
			Process process = runtime.exec(cmdarray);

			// 返回命令执行结果
			return IOToolkits.readLines(process.getInputStream());
		} catch (IOException ioe)  {
			// Ignore
			return null;
		}
	}

	private static void exit() {
		LOGGER.info("=== shutdown now!");
		// 关闭资源
		MQToolkits.shutdown();
	}

}
