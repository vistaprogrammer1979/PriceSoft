/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import com.accumed.pricing.model.request.CodeType;
import java.util.Date;

/**
 *
 * @author wfakhra
 */
public class CusPriceListItem {
    private Long id;
    private Long pricListId;
    private String code;
    private CodeType type;
    private Double price;
    private Double discount;
    private Date startDate;
    private Date endDate;
    private Boolean deleted;
     private Status status;
     private Status UpdatedStatus;

    public CusPriceListItem() {
    }

    public CusPriceListItem(Long id, Long pricListId, String code, CodeType type, 
            Double price, Double discount, Date startDate, Date endDate) {
        this.id = id;
        this.pricListId = pricListId;
        this.code = code;
        this.type = type;
        this.price = price;
        this.discount = discount;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public Double getDiscount() {
        return discount;
    }

    public void setDiscount(Double discount) {
        this.discount = discount;
    }

    public Long getPricListId() {
        return pricListId;
    }

    public void setPricListId(Long pricListId) {
        this.pricListId = pricListId;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getUpdatedStatus() {
        return UpdatedStatus;
    }

    public void setUpdatedStatus(Status UpdatedStatus) {
        this.UpdatedStatus = UpdatedStatus;
    }

     

    

     
    
    
}
