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
public class CusContract {

    private Long ID;
    private Long priceListId;
    private String insurerLicense;
    private String facilityLicense;
    private String packageName;
    private String clinicianLicense;
    private Date startDate;
    private Date endDate;
    private Boolean approved;
    private Boolean deleted;
    private Double PHARM_DISCOUNT;
    private Double IP_DISCOUNT;
    private Double OP_DISCOUNT;
    private Double BASE_RATE;
    private Integer regulator;
    private Double GAP;
    private Double MARGINAL;
    private Boolean dental;
    private Integer multipleProcedure;
    private Double primaryProcedure;
    private Double secondaryProcedure;
    private Double thirdProcedure;
    private Double forthProcedure;
    private Double IP_DRGFactor;
    private Double dayCase_DRGFactor;
    private Status status;
    private Status updatedStatus;
    private Double hspcsMarkUp;

    public CusContract() {
    }

    public CusContract(Long ID, Long priceListId, String insurerLicense,
            String facilityLicense, String packageName, String clinicianLicense,
            Date startDate, Date endDate, Boolean approved, Boolean deleted,
            Double PHARM_DISCOUNT, Double IP_DISCOUNT, Double OP_DISCOUNT, Double BASE_RATE,
            Integer regulator, Double GAP, Double MARGINAL, Integer dental,
            Integer multipleProcedure, Double primaryProcedure, Double secondaryProcedure,
            Double thirdProcedure, Double forthProcedure, Double IP_DRGFactor,Double dayCase_DRGFactor,Double hspcsMarkUp) {
        this.ID = ID;
        this.priceListId = priceListId;
        this.insurerLicense = insurerLicense;
        this.facilityLicense = facilityLicense;
        this.packageName = packageName;
        this.clinicianLicense = clinicianLicense;
        this.startDate = startDate;
        this.endDate = endDate;
        this.approved = approved;
        this.deleted = deleted;
        this.PHARM_DISCOUNT = PHARM_DISCOUNT;
        this.IP_DISCOUNT = IP_DISCOUNT;
        this.OP_DISCOUNT = OP_DISCOUNT;
        this.BASE_RATE = BASE_RATE;
        this.regulator = regulator;
        this.GAP = GAP;
        this.MARGINAL = MARGINAL;
        this.dental = (dental == 1);
        this.multipleProcedure = multipleProcedure;
        this.primaryProcedure = primaryProcedure;
        this.secondaryProcedure = secondaryProcedure;
        this.thirdProcedure = thirdProcedure;
        this.forthProcedure = forthProcedure;
        this.IP_DRGFactor = IP_DRGFactor;
        this.dayCase_DRGFactor=  dayCase_DRGFactor;
        this.hspcsMarkUp=hspcsMarkUp;
    }

//    public CusContract(Long ID, Long priceListId, String insurerLicense,
//            String facilityLicense, String packageName, String clinicianLicense,
//            Date startDate, Date endDate, Boolean approved, Boolean deleted,
//            Double PHARM_DISCOUNT, Double IP_DISCOUNT, Double OP_DISCOUNT, Double BASE_RATE,
//            Integer regulator, Double GAP, Double MARGINAL, Integer dental, Integer multipleProc,
//            Double primaryProc, Double secondaryProc, Double thirdProc, Double forthProc, Double IP_DRGFactor,Double dayCase_DRGFactor,Double hspcsMarkUp){ {
//        this.ID = ID;
//        this.priceListId = priceListId;
//        this.insurerLicense = insurerLicense;
//        this.facilityLicense = facilityLicense;
//        this.packageName = packageName;
//        this.clinicianLicense = clinicianLicense;
//        this.startDate = startDate;
//        this.endDate = endDate;
//        this.approved = approved;
//        this.deleted = deleted;
//        this.PHARM_DISCOUNT = PHARM_DISCOUNT;
//        this.IP_DISCOUNT = IP_DISCOUNT;
//        this.OP_DISCOUNT = OP_DISCOUNT;
//        this.BASE_RATE = BASE_RATE;
//        this.regulator = regulator;
//        this.GAP = GAP;
//        this.MARGINAL = MARGINAL;
//        this.dental = (dental == 1);
//        this.multipleProcedure = multipleProc;
//        this.primaryProcedure = primaryProc;
//        this.secondaryProcedure = secondaryProc;
//        this.thirdProcedure = thirdProc;
//        this.forthProcedure = forthProc;
//        this.IP_DRGFactor=IP_DRGFactor;
//        this.dayCase_DRGFactor=dayCase_DRGFactor;
//  this.hspcsMarkUp=hspcsMarkUp;
//
//    }

//    public CusContract(Long ID, Long priceListId, String insurerLicense, String facilityLicense, String packageName, String clinicianLicense, Date startDate,
//            Date endDate, Boolean approved, Boolean deleted, Double PHARM_DISCOUNT, Double IP_DISCOUNT, Double OP_DISCOUNT, Double BASE_RATE, 
//            Integer regulator, Double GAP, Double MARGINAL, Integer dental, Integer multipleProc, Double primaryProc, Double secondaryProc, 
//            Double thirdProc, Double forthProc, Double IP_PHARM_DISCOUNT, Double OP_PHARM_DISCOUNT) {
//        this.ID = ID;
//        this.priceListId = priceListId;
//        this.insurerLicense = insurerLicense;
//        this.facilityLicense = facilityLicense;
//        this.packageName = packageName;
//        this.clinicianLicense = clinicianLicense;
//        this.startDate = startDate;
//        this.endDate = endDate;
//        this.approved = approved;
//        this.deleted = deleted;
//        this.PHARM_DISCOUNT = PHARM_DISCOUNT;
//        this.IP_DISCOUNT = IP_DISCOUNT;
//        this.OP_DISCOUNT = OP_DISCOUNT;
//        this.BASE_RATE = BASE_RATE;
//        this.regulator = regulator;
//        this.GAP = GAP;
//        this.MARGINAL = MARGINAL;
//        this.dental = (dental == 1);
//        this.multipleProcedure = multipleProc;
//        this.primaryProcedure = primaryProc;
//        this.secondaryProcedure = secondaryProc;
//        this.thirdProcedure = thirdProc;
//        this.forthProcedure = forthProc;
//    }

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

    public Double getIP_DRGFactor() {
        return IP_DRGFactor;
    }

    public void setIP_DRGFactor(Double IP_DRGFactor) {
        this.IP_DRGFactor = IP_DRGFactor;
    }

    public Double getDayCase_DRGFactor() {
        return dayCase_DRGFactor;
    }

    public void setDayCase_DRGFactor(Double dayCase_DRGFactor) {
        this.dayCase_DRGFactor = dayCase_DRGFactor;
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

    public Boolean isDental() {
        return dental;
    }

    public void setDental(Boolean dental) {
        this.dental = dental;
    }

    public Integer isMultipleProcedure() {
        return multipleProcedure;
    }

    public Integer getMultipleProcedure() {
        return multipleProcedure;
    }

    public void setMultipleProcedure(Integer multipleProcedure) {
        this.multipleProcedure = multipleProcedure;
    }

    public Double getPrimaryProcedure() {
        return primaryProcedure;
    }

    public void setPrimaryProcedure(Double primaryProcedure) {
        this.primaryProcedure = primaryProcedure;
    }

    public Double getSecondaryProcedure() {
        return secondaryProcedure;
    }

    public void setSecondaryProcedure(Double secondaryProcedure) {
        this.secondaryProcedure = secondaryProcedure;
    }

    public Double getThirdProcedure() {
        return thirdProcedure;
    }

    public void setThirdProcedure(Double thirdProcedure) {
        this.thirdProcedure = thirdProcedure;
    }

    public Double getForthProcedure() {
        return forthProcedure;
    }

    public Status getUpdatedStatus() {
        return updatedStatus;
    }

    public void setUpdatedStatus(Status updatedStatus) {
        this.updatedStatus = updatedStatus;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Double getHspcsMarkUp() {
        return hspcsMarkUp;
    }

    public void setHspcsMarkUp(Double hspcsMarkUp) {
        this.hspcsMarkUp = hspcsMarkUp;
    }

    public void setForthProcedure(Double forthProcedure) {
        this.forthProcedure = forthProcedure;
    }

}
