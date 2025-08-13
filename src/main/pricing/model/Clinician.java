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
public class Clinician {

    private String license;
    private String facility_license;
    private Date validFrom;
    private Date validTo;
    public String status;
    public String profession;
    public String category;
    public String facility_type;
    public Integer regulator;

    public Clinician() {
    }

    public Clinician(String license, String facility_license, Date validFrom, Date validTo, String status, String profession, String category, String facility_type, Integer regulator) {
        this.license = license;
        this.facility_license = facility_license;
        this.validFrom = validFrom;
        this.validTo = validTo;
        this.status = status;
        this.profession = profession;
        this.category = category;
        this.facility_type = facility_type;
        this.regulator = regulator;
    }

    public String getFacility_license() {
        return facility_license;
    }

    public void setFacility_license(String facility_license) {
        this.facility_license = facility_license;
    }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public Date getValidFrom() {
        return validFrom;
    }

    public void setValidFrom(Date validFrom) {
        this.validFrom = validFrom;
    }

    public Date getValidTo() {
        return validTo;
    }

    public void setValidTo(Date validTo) {
        this.validTo = validTo;
    }

    public String getStatus() {

        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getProfession() {
        return profession;
    }

    public void setProfession(String profession) {
        this.profession = profession;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getFacility_type() {
        return facility_type;
    }

    public void setFacility_type(String facility_type) {
        this.facility_type = facility_type;
    }

    public Integer getRegulator() {
        return regulator;
    }

    public void setRegulator(Integer regulator) {
        this.regulator = regulator;
    }
    
    
}
