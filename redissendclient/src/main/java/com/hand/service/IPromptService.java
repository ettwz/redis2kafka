package com.hand.service;

import com.hand.dao.PromptDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by DongFan on 2016/11/16.
 */
public interface IPromptService {
    public String getMsg(String msgCode);
    public void add(String msgCode, String msg);
}
