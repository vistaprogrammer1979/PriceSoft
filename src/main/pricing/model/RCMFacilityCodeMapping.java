/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

/**
 *
 * @author wfakhra
 */
public class RCMFacilityCodeMapping {

    private String activityCode;
  //  private int type;
    private double price;
    private String  providerActivityCode;
    private int type;
    private String   facilityLisence;

    public RCMFacilityCodeMapping(String activityCode, double price, String providerActivityCode, int type, String facilityLisence) {
        this.activityCode = activityCode;
        this.price = price;
        this.providerActivityCode = providerActivityCode;
        this.type = type;
        this.facilityLisence = facilityLisence;
    }

    public String getActivityCode() {
        return activityCode;
    }

    public void setActivityCode(String activityCode) {
        this.activityCode = activityCode;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getProviderActivityCode() {
        return providerActivityCode;
    }

    public void setProviderActivityCode(String providerActivityCode) {
        this.providerActivityCode = providerActivityCode;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getFacilityLisence() {
        return facilityLisence;
    }

    public void setFacilityLisence(String facilityLisence) {
        this.facilityLisence = facilityLisence;
    }

   }
