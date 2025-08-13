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
public class MasterPriceListItem {
    private long id;
    private long masterListId;
    private String code;
    private CodeType type;
    private Double price;
    private Date startDate;
    private Date endDate;
    private Integer anaesthesiaBaseUnits;
    private Boolean deleted;
    private Status status;
    public MasterPriceListItem() {
    }

    public MasterPriceListItem(Long id, long masterListId, String code, CodeType type, Double price,
            Date startDate, Date endDate, Integer anaesthesiaBaseUnits) {
        this.id = id;
        this.masterListId = masterListId;
        this.code = code;
        this.type = type;
        this.price = price;
        this.startDate = startDate;
        this.endDate = endDate;
        this.anaesthesiaBaseUnits = anaesthesiaBaseUnits;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public long getMasterListId() {
        return masterListId;
    }

    public void setMasterListId(long masterListId) {
        this.masterListId = masterListId;
    }

    public long getMasterList() {
        return masterListId;
    }

    public void setMasterList(long masterListId) {
        this.masterListId = masterListId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
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

    public Integer getAnaesthesiaBaseUnits() {
        return anaesthesiaBaseUnits;
    }

    public void setAnaesthesiaBaseUnits(Integer anaesthesiaBaseUnits) {
        this.anaesthesiaBaseUnits = anaesthesiaBaseUnits;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    
    
}
