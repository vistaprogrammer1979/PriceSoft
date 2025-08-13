/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import com.accumed.pricing.model.request.CodeType;
import java.util.Date;

/**
 *
 * @author smutlak
 */
public class SPCCodeFactor {
    private Long ID;
    private Integer priceListId;
    private CodeType type;
    private String code;
    private Double factor;
    private Date startDate;
    private Date endDate;

    public SPCCodeFactor() {
    }

    public SPCCodeFactor(Long ID, Integer priceListId, CodeType type, String code, Double factor, Date startDate, Date endDate) {
        this.ID = ID;
        this.priceListId = priceListId;
        this.type = type;
        this.code = code;
        this.factor = factor;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public Integer getPriceListId() {
        return priceListId;
    }

    public void setPriceListId(Integer priceListId) {
        this.priceListId = priceListId;
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getFactor() {
        return factor;
    }

    public void setFactor(Double factor) {
        this.factor = factor;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    
}
