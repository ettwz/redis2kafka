package com.hand.util.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper {

	@Resource(name = "objectMapper")
	protected ObjectMapper objectMapper;

	public void asRuntime(Throwable throwable) {
		if (throwable instanceof RuntimeException) {
			throw (RuntimeException) throwable;
		}
		throw new RuntimeException(throwable);
	}

	public List<Map<String, ?>> convertToList(Collection<String> jsons) {
		List<Map<String, ?>> list = new ArrayList<>();
		try {
			for (String json : jsons){
				if(json!=null){
					list.add(objectMapper.readValue(json, HashMap.class));
				}
			}
		} catch (IOException e) {
			asRuntime(e);
		}
		return list;
	}

	public String convertToJson(Map<String, ?> map) {
		try {
			return objectMapper.writeValueAsString(map);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	} 

	public Map<String, ?> convertToMap(String json) {
		try {
			return objectMapper.readValue(json, HashMap.class);
		} catch (IOException e) {
			asRuntime(e);
		}
		return null;
	}
}
