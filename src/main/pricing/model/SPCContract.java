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
public class SPCContract {
       private Long ID;
       private Integer type;
       private String insurerLicense;
       private String facilityLicense;
       private String packageName;
       private String clinicianLicense;
       private Date startDate;
       private Date endDate;
       private Double factor;
       private Boolean approved;
       private Boolean deleted;
       private Integer parentId;
       private Double PHARM_DISCOUNT;
       private Double IP_DISCOUNT;
       private Double OP_DISCOUNT;
       private Double BASE_RATE;
       private Integer regulator;
       private Double GAP;
       private Double MARGINAL;

    public SPCContract() {
    }

    public SPCContract(Long ID, Integer type, String insurerLicense, 
            String facilityLicense, String packageName, String clinicianLicense, 
            Date startDate, Date endDate, Double factor, Boolean approved, Boolean deleted, 
            Integer parentId, Double PHARM_DISCOUNT, Double IP_DISCOUNT, 
            Double OP_DISCOUNT, Double BASE_RATE, Integer regulator, 
            Double GAP, Double MARGINAL) {
        this.ID = ID;
        this.type = type;
        this.insurerLicense = insurerLicense;
        this.facilityLicense = facilityLicense;
        this.packageName = packageName;
        this.clinicianLicense = clinicianLicense;
        this.startDate = startDate;
        this.endDate = endDate;
        this.factor = factor;
        this.approved = approved;
        this.deleted = deleted;
        this.parentId = parentId;
        this.PHARM_DISCOUNT = PHARM_DISCOUNT;
        this.IP_DISCOUNT = IP_DISCOUNT;
        this.OP_DISCOUNT = OP_DISCOUNT;
        this.BASE_RATE = BASE_RATE;
        this.regulator = regulator;
        this.GAP = GAP;
        this.MARGINAL = MARGINAL;
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getInsurerLicense() {
        return insurerLicense;
    }

    public void setInsurerLicense(String insurerLicense) {
        this.insurerLicense = insurerLicense;
    }

    public String getFacilityLicense() {
        return facilityLicense;
    }

    public void setFacilityLicense(String facilityLicense) {
        this.facilityLicense = facilityLicense;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getClinicianLicense() {
        return clinicianLicense;
    }

    public void setClinicianLicense(String clinicianLicense) {
        this.clinicianLicense = clinicianLicense;
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

    public Double getFactor() {
        return factor;
    }

    public void setFactor(Double factor) {
        this.factor = factor;
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

    public Integer getParentId() {
        return parentId;
    }

    public void setParentId(Integer parentId) {
        this.parentId = parentId;
    }

    public Double getPHARM_DISCOUNT() {
        return PHARM_DISCOUNT;
    }

    public void setPHARM_DISCOUNT(Double PHARM_DISCOUNT) {
        this.PHARM_DISCOUNT = PHARM_DISCOUNT;
    }

    public Double getIP_DISCOUNT() {
        return IP_DISCOUNT;
    }

    public void setIP_DISCOUNT(Double IP_DISCOUNT) {
        this.IP_DISCOUNT = IP_DISCOUNT;
    }

    public Double getOP_DISCOUNT() {
        return OP_DISCOUNT;
    }

    public void setOP_DISCOUNT(Double OP_DISCOUNT) {
        this.OP_DISCOUNT = OP_DISCOUNT;
    }

    public Double getBASE_RATE() {
        return BASE_RATE;
    }

    public void setBASE_RATE(Double BASE_RATE) {
        this.BASE_RATE = BASE_RATE;
    }

    public Integer getRegulator() {
        return regulator;
    }

    public void setRegulator(Integer regulator) {
        this.regulator = regulator;
    }


    public Double getGAP() {
        return GAP;
    }

    public void setGAP(Double GAP) {
        this.GAP = GAP;
    }

    public Double getMARGINAL() {
        return MARGINAL;
    }

    public void setMARGINAL(Double MARGINAL) {
        this.MARGINAL = MARGINAL;
    }
}
