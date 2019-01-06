package com.simon.credit.toolkit.mq;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.simon.credit.toolkit.common.CommonToolkits;
import com.simon.credit.toolkit.io.IOToolkits;

public class MqReissueTest {

	public static void main(String[] args) throws IOException {

		Map<String, String> custNoAndTaskIdMap = parseCustNoAndTaskId();
		System.out.println(custNoAndTaskIdMap.size());

		List<String> lines = IOToolkits.readLines(new File("d:/mq.txt"));

		int count = 0;
		List<String> resendMsgList = new ArrayList<String>(220);
		for (String line : lines) {
			line = StringUtils.replace(line, "\\", "");
			line = StringUtils.replace(line, "null", "\"\"");
			line = StringUtils.replace(line, "\"{", "{");
			line = StringUtils.replace(line, "}\"", "}");

			JSONObject jsonObj = JSON.parseObject(line);
			if ("2018090500000001".equals(jsonObj.getString("appId")) && 
				CommonToolkits.isEmptyContainNull(jsonObj.getJSONObject("bizContent").getString("taskId"))) {
				// System.out.println(line);
				count++;
				String custNo = jsonObj.getString("userId");
				// System.out.println("'" + custNo + "', ");
				jsonObj.getJSONObject("bizContent").put("taskId",custNoAndTaskIdMap.get(custNo));
				//System.out.println(jsonObj.toJSONString());
				resendMsgList.add(jsonObj.toJSONString());
			}
		}

		FileUtils.writeLines(new File("d:/resendMQMsg.txt"), CommonToolkits.UTF8, resendMsgList, "\n");
		System.out.println("total: " + count);
	}

	private static Map<String, String> parseCustNoAndTaskId() {
		Map<String, String> map = new HashMap<String, String>(220);

		List<String> lines = IOToolkits.readLines(new File("d:/creditParam.txt"));
		for (String line : lines) {
			JSONObject jsonObj = JSON.parseObject(line);
			map.put(jsonObj.getString("custNo"), jsonObj.getString("taskId"));
		}

		return map;
	}

}
