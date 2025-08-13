/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.io.Serializable;

/**
 *
 * @author smutlak
 */
public class ObseleteClaimInfo implements Serializable{
    
    private Long id;
    private String payerLicense;
    private String receiverLicense;
    private String facilityLicense;
    private String packageName;
    private Long patientInsuranceId; 
    private Integer encounterType;

    public ObseleteClaimInfo() {
    }

    public ObseleteClaimInfo(Long id, String payerLicense, String receiverLicense, String facilityLicense, String packageName, Long patientInsuranceId, Integer encounterType) {
        this.id = id;
        this.payerLicense = payerLicense;
        this.receiverLicense = receiverLicense;
        this.facilityLicense = facilityLicense;
        this.packageName = packageName;
        this.patientInsuranceId = patientInsuranceId;
        this.encounterType = encounterType;
    }

    public Integer getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(Integer encounterType) {
        this.encounterType = encounterType;
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

    public Long getPatientInsuranceId() {
        return patientInsuranceId;
    }

    public void setPatientInsuranceId(Long patientInsuranceId) {
        this.patientInsuranceId = patientInsuranceId;
    }

    public String getPayerLicense() {
        return payerLicense;
    }

    public void setPayerLicense(String payerLicense) {
        this.payerLicense = payerLicense;
    }

    public String getReceiverLicense() {
        return receiverLicense;
    }

    public void setReceiverLicense(String receiverLicense) {
        this.receiverLicense = receiverLicense;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    
    
    
}
