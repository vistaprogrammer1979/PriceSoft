/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.accumed.pricing.model;

import java.io.Serializable;

/**
 *
 * @author smutlak
 * 
 * Select * from PL_SPC_PriceList SPC 
INNER JOIN ACCUMED_FACILITY F on F.facility_license = SPC.FACILITY_LICENSE
LEFT JOIN ACCUMED_CLINICIANS C ON C.Facility_License = SPC.FACILITY_LICENSE
where SPC.FACILITY_LICENSE = 'MF23'

Select * from PL_CUS_Contract CUS 
INNER JOIN ACCUMED_FACILITY F on F.facility_license = CUS.FACILITY_LICENSE
LEFT JOIN ACCUMED_CLINICIANS C ON C.Facility_License = CUS.FACILITY_LICENSE
where CUS.FACILITY_LICENSE = 'DHA-F-0047259'


Select FACILITY_LICENSE from ACCUMED_FACILITY where FACILITY_LICENSE not in(
Select distinct Facility_License from ACCUMED_CLINICIANS)
 */
public class Facility implements Serializable{
    
    private String license;
    private FacilityType type;
    private Regulator regulator;
    private Boolean active;
   
    

    public Facility() {
    }

    public Facility(String license) {
        this.license = license;
    }

    public Facility(String license, FacilityType type, Regulator regulator, Boolean active) {
        this.license = license;
        this.type = type;
        this.regulator = regulator;
        this.active = active;
    }
    
    

   

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    

    
    public FacilityType getType() {
        return type;
    }

    public void setType(FacilityType type) {
        this.type = type;
    }

    public Regulator getRegulator() {
        return regulator;
    }

    public void setRegulator(Regulator regulator) {
        this.regulator = regulator;
    }

    public Boolean isActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    
    
    
}
