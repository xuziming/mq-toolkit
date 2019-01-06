package com.simon.credit.toolkit.mq;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.simon.credit.toolkit.common.CommonToolkits;
import com.simon.credit.toolkit.io.IOToolkits;

/**
 * MQ消息工具
 * @author XUZIMING 2017-12-28
 */
public class MQToolkits {
	private static final Logger LOGGER = LoggerFactory.getLogger(MQToolkits.class);
	private static DefaultMQProducer producer;

	static {
		// Linux系统下执行MQ生产者初始化
		if (SystemTookits.isLinuxSystem()) {
			try {
				producer = new DefaultMQProducer("credit_risk_management");
				producer.setNamesrvAddr(parseRocketmqNameServer());
				producer.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 发送MQ消息
	 * @param topic MQ消息主题
	 * @param msgBody MQ消息体
	 */
	public static SendResult sendMessge(String topic, String msgBody) {
		if (producer == null) {
			return null;
		}

		LOGGER.info("=== topic: {}, msgBody: {}", topic, msgBody);
		try {
			Message message = new Message(topic, CommonToolkits.toBinary(msgBody));

			SendResult sendResult = producer.send(message);
			LOGGER.info("=== sendResult: {}", JSON.toJSONString(sendResult));
			return sendResult;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 解析RocketMQ Name Server地址
	 * @return
	 * @throws IOException
	 */
	private static String parseRocketmqNameServer() throws IOException {
		// 执行此命令有个前提: 连接rocketmq服务器的应用已经启动, 否则查询结果为空.
		// String command = "netstat -anpl | grep 9876 | awk '{print $5}'";

		String command = "cat /etc/hosts | grep jmenv.tbsite.net | awk '{print $1}'";
		String nginxIP = executeLinuxCommand(command).get(0);

		String nameServer = executeLinuxCommand("curl http://" + nginxIP + ":8080/rocketmq/nsaddr").get(0);
		LOGGER.info("=== rocketmq name server: " + nameServer);
		return nameServer;
	}

	/**
	 * 执行Linux命令
	 * @param command 命令行
	 * @return
	 * @throws IOException
	 */
	private static List<String> executeLinuxCommand(String command) throws IOException {
		Runtime runtime = Runtime.getRuntime();
		String[] cmdarray = { "/bin/sh", "-c", command };
		Process process = runtime.exec(cmdarray);

		return IOToolkits.readLines(process.getInputStream());
	}

	/**
	 * 停止MQ发送服务
	 */
	public static void shutdown() {
		if (producer != null) {
			producer.shutdown();
		}
	}

}
