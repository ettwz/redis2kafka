package com.hand.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.hand.service.IPromptService;
import com.hand.service.impl.PromptServiceImpl;
import javax.annotation.Resource;
import java.util.List;

/**
 * 数据返回对象.
 * Created by DongFan on 2016/11/15.
 *
 * @author fan.dong@hand.china.com
 */
public class ResponseData {

    @Resource(name = "promptService")
    private IPromptService promptService = new PromptServiceImpl();

    // 返回状态编码
    private String msgCode;

    // 返回信息
    private String msg;

    //数据
    @JsonInclude(Include.NON_NULL)
    private List<?> resp;

    // 成功标识
    private boolean success = true;

    //数据
    @JsonInclude(Include.NON_NULL)
    private String back;

    public ResponseData() {
    }

    public ResponseData(boolean success) {
        setSuccess(success);
    }

    public ResponseData(List<?> list) {
        this(true);
        setResp(list);
    }

    public String getMsgCode() {
        return msgCode;
    }

    public String getBack() {
        return back;
    }

    public String getMsg() {
        return msg;
    }

    public List<?> getResp() {
        return resp;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setBack(String back) {
        this.back = back;
    }

    public void setMsgCode(String msgCode) {
        this.msgCode = msgCode;
        // this.setMsg(promptService.getMsg(msgCode));
    }

    private void setMsg(String msg) {
        this.msg = msg;
    }

    public void setResp(List<?> resp) {
        this.resp = resp;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
