package com.bonc.dw3.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by zg on 2017/8/17.
 */
public class DescartesUtils {
    /**
     * 递归实现dimValue中的笛卡尔积，结果放在result中
     *
     * @param dimValue 原始数据
     * @param result   结果数据
     * @param layer    dimValue的层数
     * @param curList  每次笛卡尔积的结果
     */
    public static void recursive(List<List<String>> dimValue, List<List<String>> result, int layer, List<String> curList) {
        if (layer < dimValue.size() - 1) {
            if (dimValue.get(layer).size() == 0) {
                recursive(dimValue, result, layer + 1, curList);
            } else {
                for (int i = 0; i < dimValue.get(layer).size(); i++) {
                    List<String> list = new ArrayList<String>(curList);
                    list.add(dimValue.get(layer).get(i));
                    recursive(dimValue, result, layer + 1, list);
                }
            }
        } else if (layer == dimValue.size() - 1) {
            if (dimValue.get(layer).size() == 0) {
                result.add(curList);
            } else {
                for (int i = 0; i < dimValue.get(layer).size(); i++) {
                    List<String> list = new ArrayList<String>(curList);
                    list.add(dimValue.get(layer).get(i));
                    result.add(list);
                }
            }
        }
    }

    /**
     * 循环实现dimValue中的笛卡尔积，结果放在result中
     *
     * @param dimValue 原始数据
     * @param result   结果数据
     */
    public static void circulate(List<List<String>> dimValue, List<List<String>> result) {
        int total = 1;
        for (List<String> list : dimValue) {
            total *= list.size();
        }
        String[] myResult = new String[total];

        int itemLoopNum = 1;
        int loopPerItem = 1;
        int now = 1;
        for (List<String> list : dimValue) {
            now *= list.size();

            int index = 0;
            int currentSize = list.size();

            itemLoopNum = total / now;
            loopPerItem = total / (itemLoopNum * currentSize);
            int myIndex = 0;

            for (String string : list) {
                for (int i = 0; i < loopPerItem; i++) {
                    if (myIndex == list.size()) {
                        myIndex = 0;
                    }

                    for (int j = 0; j < itemLoopNum; j++) {
                        myResult[index] = (myResult[index] == null ? "" : myResult[index] + ",") + list.get(myIndex);
                        index++;
                    }
                    myIndex++;
                }

            }
        }
        String[] tmpStrArr;
        int splitNum=0;
        List<String> stringResult = Arrays.asList(myResult);
        for (String string : stringResult) {
            StringTokenizer st = new StringTokenizer(string,",");
//            String[] stringArray = string.split(",");
            tmpStrArr = new String[st.countTokens()];

                while (st.hasMoreTokens()) {
                    tmpStrArr[splitNum++] = st.nextToken();
                }
            splitNum=0;
            result.add(Arrays.asList(tmpStrArr));
        }
    }
}
