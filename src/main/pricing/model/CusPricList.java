/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

//import com.accumed.pricing.DAO.DAO;
import java.util.Date;
import java.util.List;

/**
 *
 * @author smutlak
 */
public class CusPricList {
    private Long ID;
    private Date startDate;
    private Date endDate;
    private Boolean approved;
    private Boolean deleted;
    private Boolean dental;
    private String name;
    private Integer regulator;
    protected List<CusPriceListItem> items;

    public CusPricList() {
    }

    public CusPricList(Long ID, Date startDate, Date endDate, Boolean approved, 
            Boolean deleted, Integer dental, String name, Integer regulator) {
        this.ID = ID;
        this.startDate = startDate;
        this.endDate = endDate;
        this.approved = approved;
        this.deleted = deleted;
        //this.priceListType = priceListType;
        this.dental = (dental==1);
        this.name = name;
        this.regulator = regulator;
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
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

    public Boolean getApproved() {
        return approved;
    }

    public void setApproved(Boolean approved) {
        this.approved = approved;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Boolean isDental() {
        return dental;
    }

    public void setDental(Boolean dental) {
        this.dental = dental;
    }

    

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getRegulator() {
        return regulator;
    }

    public void setRegulator(Integer regulator) {
        this.regulator = regulator;
    }

//    public List<CusPriceListItem> getItems(DAO dao) {
//        if(items == null){
//            items = dao.getCustomPricelistItems(this);
//        }
//        return items;
//    }
    public List<CusPriceListItem> getItems() {
       return items;
    }

    public void setItems(List<CusPriceListItem> items) {
        this.items = items;
    }
    
}
