package com.hand.service.impl;

import com.hand.dao.PromptDao;
import com.hand.service.IPromptService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DongFan on 2016/11/16.
 */
@Service("promptService")
public class PromptServiceImpl implements IPromptService {

    @Resource(name = "promptDao")
    private PromptDao promptDao = new PromptDao();

    @Override
    public String getMsg(String msgCode) {
        return (String) promptDao.select(msgCode).get("msg");
    }

    @Override
    public void add(String msgCode, String msg) {
        Map<String, String> map = new HashMap<>();
        map.put("msgCode", msgCode);
        map.put("msg", msg);
        promptDao.add(map);
    }
}
