package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by zg on 2017/8/15.
 */
@Service
@CrossOrigin(origins = "*")
public class ComputeServiceImpl implements ComputeService{

    private static Logger log = LoggerFactory.getLogger(ComputeServiceImpl.class);

    /**
     * 核心计算逻辑，用到groupby，汇总字段，查询字段
     * @param dataObject 构造的查询条件
     * @param dataList 原始数据
     * @return
     */
    @Override
    public List<Map<String, Object>> compute(DataObject dataObject, List<Map<String, Object>> dataList) {
        List<Map<String, Object>> result = new ArrayList<>();
        if(null == dataList || dataList.size() == 0){
            return result;
        }
        //查询字段
        List<String> queryFiledList = dataObject.getQueryFieldList();
        //查询字段别名
        List<String> queryFieldAliasList = dataObject.getQueryFieldAliasList();
        //sum汇总字段
        List<String> sumFieldList = dataObject.getSumFieldList();
        //sum汇总字段别名
        List<String> sumFieldAliasList = dataObject.getSumFieldAliasList();
        //groupBy字段
        Set<String> groupbySet = dataObject.getGroupbySet();
        if(validateField(queryFiledList, sumFieldList, groupbySet, dataList)){
            result = sumAndGroupbyRaw(queryFiledList, queryFieldAliasList,sumFieldList, sumFieldAliasList, groupbySet, dataList);
        }
        return result;
    }

    /**
     * 校验字段，数据里必须包含其它字段，否则无法进行计算
     * @param queryFiledList
     * @param sumFieldList
     * @param groupbySet
     * @param dataList
     * @return
     */
    private boolean validateField(List<String> queryFiledList, List<String> sumFieldList, Set<String> groupbySet,
                                                    List<Map<String, Object>> dataList){
        //避免sum的字段出现null
        List<String> tmpList = removeNullEle(sumFieldList);
        Set<String> keySet = dataList.get(0).keySet();

        if(!keySet.containsAll(queryFiledList)){
            log.error("结果集中不包含查询字段，" + keySet + "," + queryFiledList);
            return false;
        }
        if(!keySet.containsAll(tmpList)){
            log.error("结果集中不包含sum字段，" + keySet + "," + sumFieldList);
            return false;
        }
        if(!keySet.containsAll(groupbySet)){
            log.error("结果集中不包含groupby字段，" + keySet + "," + groupbySet);
            return false;
        }
        return true;
    }

    /**
     * 移除list中为null的元素
     * @param paramList
     * @return
     */
    static List<String> removeNullEle(List<String> paramList) {
        List<String> tmpList = new ArrayList<>(paramList);
        tmpList.removeAll(Collections.singleton(null));
        return tmpList;
    }

    /**
     * 使用原始数据进行处理
     * @param queryFiledList
     * @param queryFieldAliasList
     * @param sumFieldList
     * @param sumFieldAliasList
     * @param groupbySet
     * @param dataList
     * @return
     */
    private List<Map<String, Object>> sumAndGroupbyRaw(List<String> queryFiledList, List<String> queryFieldAliasList,List<String> sumFieldList, List<String> sumFieldAliasList, Set<String> groupbySet,
            List<Map<String, Object>> dataList){
        Map<String,Object> rawMap =  sumAndGroupby(sumFieldList,groupbySet,dataList);
        List<Map<String,Object>> resultList = formatRawMap(rawMap,queryFiledList,queryFieldAliasList,sumFieldList,sumFieldAliasList,groupbySet);
        return resultList;
    }

    /**
     * 多字段sum操作
     * @param sumFieldList
     * @param groupbySet
     * @param dataList
     * @return
     */
    static Map<String,Object> sumAndGroupby(List<String> sumFieldList,Set<String> groupbySet,List<Map<String,Object>> dataList)
    {
        //判定查询条件和sum字段无误
        Map<String,Object> singleMap = dataList.get(0);
        Set singleSet = singleMap.keySet();
        if(!singleSet.containsAll(groupbySet)||!singleSet.containsAll(removeNullEle(sumFieldList)))//说明参量的keys是数据map中所有key的真子集。
            return new HashMap();
        Iterator iterator = dataList.iterator();
        Object[] groupbyArr = groupbySet.toArray();
        //用于接收结果，此map的key为groupby字段，value为各sum字段结果（求和），k-v均由竖线分割
        Map<String,Object> resultMap = new HashMap();
        StringBuffer groupbyKey = new StringBuffer();
        Object[] sumArr = sumFieldList.toArray();
        StringBuffer sumValue = new StringBuffer();
        //使用Bigdecimal接收数字数据
        DecimalFormat decimalFormat = new DecimalFormat("#0.00000");
        decimalFormat.setRoundingMode(RoundingMode.UP);
        //遍历原始数据
        while (iterator.hasNext()){
            singleMap = (Map<String, Object>) iterator.next();
            //遍历数据的groupby字段，取该字段在单条数据中对应，存入groupbyKey且竖线拼接
            for(int i=0;i<groupbyArr.length;i++)
            {
                groupbyKey.append(singleMap.get(groupbyArr[i]).toString())
                        .append("|");
            }
            //如果groupby结果集中有和当前groupbyKey匹配的结果，则说明找到分组目标，指定sum对应字段进行累加。
            if(resultMap.containsKey(groupbyKey.toString()))
            {
                StringTokenizer st = new StringTokenizer((String)resultMap.get(groupbyKey.toString()),"|");
                String sumFieldValue = new String();
                for(int i =0;i<sumArr.length;i++){
                    if (null == sumArr[i]){
                        sumValue.append(null+"|");
                        st.nextToken();
                    }else {
                        String nxToken = st.nextToken();
                        if (null != singleMap.get(sumArr[i].toString()) && !"".equals(singleMap.get(sumArr[i].toString()).toString())){
                            sumFieldValue = singleMap.get(sumArr[i].toString()).toString();
                            if (!"null".equals(nxToken)){
                                sumValue.append(decimalFormat.format(new BigDecimal(sumFieldValue).doubleValue() +(new BigDecimal(nxToken).doubleValue())))
                                        .append("|");
                            }
                        }else {
                            if (!"null".equals(nxToken)) {
                                sumValue.append(decimalFormat.format(0 + (new BigDecimal(nxToken).doubleValue())))
                                        .append("|");
                            }
                        }
                    }
                }
                resultMap.put(groupbyKey.toString(),sumValue.toString());
                sumValue.setLength(0);
            }
            else//如果groupby结果集没有和当前groupbyKey匹配的结果，说明该分组为新分组，并插入sum对应字段数据。
            {
                for(int i =0;i<sumArr.length;i++) {
                    if (null == sumArr[i]){
                        sumValue.append(null+"|");
                    }else if (null != singleMap.get(sumArr[i].toString()) && !"".equals(singleMap.get(sumArr[i].toString()).toString())){
                        sumValue.append(decimalFormat.format(new BigDecimal(singleMap.get(sumArr[i].toString()).toString()).doubleValue())).append("|");
                    }else {
                        sumValue.append(0).append("|");
                    }
                }
                resultMap.put(groupbyKey.toString(),sumValue.toString());
                sumValue.setLength(0);
            }
            groupbyKey.setLength(0);
        }
        return resultMap;
    }

    /**
     * 格式化并返回sql结果形式的数据，添加别名等
     * @param rawMap
     * @param sumFieldList
     * @param sumFieldAliasList
     * @param groupbySet
     * @return
     */
    static List<Map<String,Object>> formatRawMap(Map<String,Object> rawMap,List<String> queryFiledList,List<String> queryFieldAliasList,List<String> sumFieldList,List<String> sumFieldAliasList,Set<String> groupbySet)
    {
        List<Map<String,Object>> resultList = new ArrayList();
        Object[] sumAliasArr = sumFieldAliasList.toArray();
//        Object[] sumAliasArr = unifyOrder(sumFieldList,sumFieldAliasList);
        Object[] groupbyArr = groupbySet.toArray();
        Set<Map.Entry<String, Object>> entrySet = rawMap.entrySet();
        Map<String,Object> elment = new HashMap();
        for (Map.Entry<String,Object> entry:entrySet){
            String keyV = entry.getKey();
            String valueV = (String)entry.getValue();
            StringTokenizer stK = new StringTokenizer(keyV,"|");
            StringTokenizer stV = new StringTokenizer(valueV,"|");
            //如果groupby字段在query字段中，则予以返回，并判断是否有别名
            for(int i=0;i<groupbyArr.length;i++)
            {
                String nxToken = stK.nextToken();
                for (int j=0;j<queryFiledList.size();j++){
                    if (queryFiledList.get(j).equals(groupbyArr[i].toString())){
                        if (null != queryFieldAliasList && !queryFieldAliasList.isEmpty()){
                            elment.put(queryFieldAliasList.get(j),nxToken);
                        }else {
                            elment.put(queryFiledList.get(j), nxToken);
                        }
                    }
                }
            }
            //sum字段如果出现null，该字段的值赋为null
            for(int i=0;i<sumAliasArr.length;i++){
                String nxToken = stV.nextToken();
                if (!"null".equals(nxToken)){
                    elment.put(sumAliasArr[i].toString(), nxToken);
                }else {
                    elment.put(sumAliasArr[i].toString(), null);
                }
            }
            resultList.add(elment);
            elment = new HashMap();
        }
        return resultList;
    }

    static Object[] unifyOrder(List<String> sumFieldList,List<String> sumFieldAliasList){
        Set<String> fieldSet = new HashSet<>();
        fieldSet.addAll(sumFieldList);
        Object[] fieldSetArr = fieldSet.toArray();
        Object[] fieldListArr = sumFieldList.toArray();
        Object[] aliListArr = sumFieldAliasList.toArray();

        Map<String,String> map = new HashMap<>();
        List<String> indexList = new ArrayList<>();
        for (int i=0;i<fieldListArr.length;i++){
            if (null == fieldListArr[i]){
                indexList.add(String.valueOf(i));
                map.put("IndexMark"+i,aliListArr[i].toString());
            }else {
                map.put(fieldListArr[i].toString(),aliListArr[i].toString());
            }

        }
        for (int i=0;i<fieldSetArr.length;i++){
            String tmp = new String();
            if (null == fieldSetArr[i]){
                tmp = map.get("IndexMark"+indexList.get(i));
            }else {
                tmp = map.get(fieldSetArr[i]);
            }

            fieldSetArr[i] = tmp;
        }
        //返回别名数组
        return fieldSetArr;
    }
}
