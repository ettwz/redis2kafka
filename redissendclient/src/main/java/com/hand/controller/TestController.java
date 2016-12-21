package com.hand.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.redis.StaffDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Created by Hand on 2016/11/14.
 */

@RestController
public class TestController {

//    @Autowired
//    private StaffRao staffRao;
    @Autowired
    private StaffDao staffRao;
    
    private ObjectMapper objectMapper = new ObjectMapper();
    {
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    }

    @RequestMapping("/mapToString")
    public String mapToString(){

        HashMap<String,Object> map = new HashMap<>();
        map.put("test1","value1");
        map.put("test2","value2");
        map.put("test3",111);
        map.put("test4",1.09);
        String value = null;
        try {
            value = objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.err.println(value);
      return value;
    }

    @RequestMapping("jsonToMap")
    public Map<String,Object> jsonToMap(){
        String json = "{\"test4\":1.09,\"test2\":\"value2\",\"test3\":111,\"test1\":\"value1\"}";
        Map<String,Object> map = null;
        try {
            map = objectMapper.readValue(json, HashMap.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(map!=null){
            Set<String> keySet = map.keySet();
            for(String key : keySet){
                System.err.println(key + ":" + map.get(key));
            }
        }else{
            System.err.print("map is null");
        }
        return map;
    }
    @RequestMapping(value = "add")
    public String add(){
//        Staff staff1 = new Staff();
//        staff1.setId(1L);
//        staff1.setName("Alan");
//        staff1.setDept("DEV");
//        staff1.setType("NORMAL");
//        staff1.setAge(21);

        HashMap<String,Object> staff1 = new HashMap<>();
        staff1.put("id",2L);
        staff1.put("name","Alan");
        staff1.put("dept","DEV");
        staff1.put("type","NORMAL");
        staff1.put("age",21);


//        Staff staff2 = new Staff();
//        staff2.setId(2L);
//        staff2.setName("Bob");
//        staff2.setDept("DEV");
//        staff2.setType("NORMAL");
//        staff2.setAge(22);

        HashMap<String,Object> staff2 = new HashMap<>();
        staff2.put("id",3L);
        staff2.put("name","Bob");
        staff2.put("dept","DEV");
        staff2.put("type","NORMAL");
        staff2.put("age",22);

//        Staff staff3 = new Staff();
//        staff3.setId(3L);
//        staff3.setName("Candy");
//        staff3.setDept("HR");
//        staff3.setType("NORMAL");
//        staff3.setAge(23);

        HashMap<String,Object> staff3 = new HashMap<>();
        staff3.put("id",4L);
        staff3.put("name","Candy");
        staff3.put("dept","HR");
        staff3.put("type","NORMAL");
        staff3.put("age",23);

//        Staff staff4 = new Staff();
//        staff4.setId(4L);
//        staff4.setName("Andy");
//        staff4.setDept("HR");
//        staff4.setType("PRAC");
//        staff4.setAge(24);

        HashMap<String,Object> staff4 = new HashMap<>();
        staff4.put("id",5L);
        staff4.put("name","Andy");
        staff4.put("dept","HR");
        staff4.put("type","PRAC");
        staff4.put("age",24);

//        Staff staff5 = new Staff();
//        staff5.setId(5L);
//        staff5.setName("Anddd");
//        staff5.setDept("MA");
//        staff5.setType("ALL");
//        staff5.setAge(27);

        HashMap<String,Object> staff5 = new HashMap<>();
        staff5.put("id",6L);
        staff5.put("name","Anddd");
        staff5.put("dept","MA");
        staff5.put("type","ALL");
        staff5.put("age",27);


//        Staff staff6 = new Staff();
//        staff6.setId(6L);
//        staff6.setName("张三");
//        staff6.setDept("MA");
//        staff6.setType("ALL");
//        staff6.setAge(29);

        HashMap<String,Object> staff6 = new HashMap<>();
        staff6.put("id",7L);
        staff6.put("name","张1111111");
        staff6.put("dept","MA");
        staff6.put("type","ALL");
        staff6.put("age",29);

//        Staff staff7 = new Staff();
//        staff7.setId(7L);
//        staff7.setName("张二");
//        staff7.setDept("MA");
//        staff7.setType("ALL");
//        staff7.setAge(30);

        HashMap<String,Object> staff7 = new HashMap<>();
        staff7.put("id",8L);
        staff7.put("name","张2222222");
        staff7.put("dept","MA");
        staff7.put("type","ALL");
        staff7.put("age",30);

//        Staff staff8 = new Staff();
//        staff8.setId(8L);
//        staff8.setName("张一");
//        staff8.setDept("MA");
//        staff8.setType("ALL");
//        staff8.setAge(31);


        HashMap<String,Object> staff8 = new HashMap<>();
        staff8.put("id",99L);
        staff8.put("name","张3333333");
        staff8.put("dept","MA");
        staff8.put("type","ALL");
        staff8.put("age",30);
        staffRao.add(staff1);
        staffRao.add(staff2);
        staffRao.add(staff3);
        staffRao.add(staff4);
        staffRao.add(staff5);
        staffRao.add(staff6);
        staffRao.add(staff7);
        staffRao.add(staff8);
        return "ok";
    }

    @RequestMapping(value = "equals")
    public List<Map<String,?>> equals(){
        String[] depts = {"DEV","HR"};
        return staffRao.selectByEqField("dept", depts);
   }

    @RequestMapping(value = "match")
    public List<Map<String,?>> match(){
        return staffRao.selectByMatchField("name", "张", 0, 5);
    }

    @RequestMapping(value = "range")
    public List<Map<String,?>> range(){
        return staffRao.selectByRangeField("age", 22, 27, 0, 10);
    }


    @RequestMapping(value = "update")
    public String update(){
//        Staff staff = new Staff();
//        staff.setAge(-1);
//        staff.setId(8L);
//        staff.setName("张零");
//        staff.setDept("MA_NEW");
//        staff.setType("ALL_NEW");
        HashMap<String,Object> staff = new HashMap<>();
        staff.put("id",8L);
        staff.put("name","张零");
        staff.put("dept","MA_NEW");
        staff.put("type","ALL_NEW");
        staff.put("age",39);
        staffRao.update(staff);
        return "ok";
    }

    @RequestMapping(value="delete/{id}")
    public String delete(@PathVariable String id){
        System.err.println(id);
        staffRao.delete(id);
        return "ok";
    }
}
