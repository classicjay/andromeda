package com.bonc.dw3.bean;

import java.util.List;

/**
 * Created by gp on 2017/8/17.
 */
public class QueryDataObject {

    //账期
    private String acctDate;
    //指标编码
    private String kpiCode;
    //省份id
    private String provId;
    //地市id
    private String areaNo;
    //渠道
    private List<String> channel;
    //合约
    private List<String> business;
    //产品
    private List<String> product;
    //查询用的表名
    private String tableName;

    public String getAcctDate() {
        return acctDate;
    }

    public void setAcctDate(String acctDate) {
        this.acctDate = acctDate;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKpiCode() {
        return kpiCode;

    }

    public void setKpiCode(String kpiCode) {
        this.kpiCode = kpiCode;
    }

    public String getProvId() {
        return provId;
    }

    public void setProvId(String provId) {
        this.provId = provId;
    }

    public String getAreaNo() {
        return areaNo;
    }

    public void setAreaNo(String areaNo) {
        this.areaNo = areaNo;
    }

    public List<String> getChannel() {
        return channel;
    }

    public void setChannel(List<String> channel) {
        this.channel = channel;
    }

    public List<String> getBusiness() {
        return business;
    }

    public void setBusiness(List<String> business) {
        this.business = business;
    }

    public List<String> getProduct() {
        return product;
    }

    public void setProduct(List<String> product) {
        this.product = product;
    }
}
