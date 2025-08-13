/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.util.Date;

/**
 *
 * @author wfakhra
 */
public class DRG {
    private String code;
    private Double weight;
    private Double portion;
    private Date startDate;
    private Date endDate;

    public DRG() {
    }

    public DRG(String code, Double weight, Double portion, Date startDate, Date endDate) {
        this.code = code;
        this.weight = weight;
        this.portion = portion;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
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

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Double getPortion() {
        return portion;
    }

    public void setPortion(Double portion) {
        this.portion = portion;
    }

    
}
