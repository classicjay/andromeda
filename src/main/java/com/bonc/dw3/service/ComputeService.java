package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;
import java.util.Map;

/**
 * Created by zg on 2017/8/14.
 */
@Service
@CrossOrigin(origins = "*")
public interface ComputeService {
    List<Map<String,Object>> compute(DataObject dataObject, List<Map<String, Object>> resultSet);
}
