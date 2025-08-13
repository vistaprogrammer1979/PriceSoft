/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.util.Date;

/**
 *
 * @author smutlak
 */
public class SPCGroupFactor {
    private Long ID;
    private Long priceListId;
    private Long groupID;
    private Double factor;
    private Date startDate;
    private Date endDate;

    public SPCGroupFactor() {
    }

    public SPCGroupFactor(Long ID, Long priceListId, Long groupID, Double factor, Date startDate, Date endDate) {
        this.ID = ID;
        this.priceListId = priceListId;
        this.groupID = groupID;
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

    public Long getPriceListId() {
        return priceListId;
    }

    public void setPriceListId(Long priceListId) {
        this.priceListId = priceListId;
    }

    public Long getGroupID() {
        return groupID;
    }

    public void setGroupID(Long groupID) {
        this.groupID = groupID;
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
