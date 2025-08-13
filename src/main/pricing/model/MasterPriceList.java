/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.util.Date;
import java.util.List;

/**
 *
 * @author smutlak
 */
public class MasterPriceList {
    private Long id;
    private String name;
    private Date startDate;
    private Date endDate;
    private Integer regulator;
    private Status status;
    public MasterPriceList() {
    }

    public MasterPriceList(Long id, String name, Date startDate, Date endDate, Integer regulator) {
        this.id = id;
        this.name = name;
        this.startDate = startDate;
        this.endDate = endDate;
        this.regulator = regulator;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Integer getRegulator() {
        return regulator;
    }

    public void setRegulatorID(Integer regulator) {
        this.regulator = regulator;
    }

//    public List<MasterPriceListItem> getItems() {
//        return items;
//    }
//
//    public void setItems(List<MasterPriceListItem> items) {
//        this.items = items;
//    }

    public void setStatus(Status status) {
        this.status = status;
    }

     

    public Status getStatus() {
        return status;
    }
}
