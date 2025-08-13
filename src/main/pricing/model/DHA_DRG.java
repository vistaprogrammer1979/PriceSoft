/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

/**
 *
 * @author wfakhra
 */
public class DHA_DRG {

    private String code;
     private String addmissionType;
    
    private Double relativeWeight;
    private Double hcpcsPortion;
    private Double drugPortion;
    private Double surgeryPortion;
    private Double aLos;
    private String description;

    public DHA_DRG() {
    }

    public DHA_DRG(String code, String addmissionType, Double relativeWeight, Double hcpcsPortion, Double drugPortion, Double surgeryPortion, Double aLos, String description) {
        this.code = code;
        this.addmissionType = addmissionType;
        this.relativeWeight = relativeWeight;
        this.hcpcsPortion = hcpcsPortion;
        this.drugPortion = drugPortion;
        this.surgeryPortion = surgeryPortion;
        this.aLos = aLos;
        this.description = description;
    }
 public DHA_DRG(String code, String addmissionType, Double relativeWeight, Double hcpcsPortion, Double drugPortion, Double surgeryPortion, Double aLos ) {
        this.code = code;
        this.addmissionType = addmissionType;
        this.relativeWeight = relativeWeight;
        this.hcpcsPortion = hcpcsPortion;
        this.drugPortion = drugPortion;
        this.surgeryPortion = surgeryPortion;
        this.aLos = aLos;
       // this.description = description;
    }
    
    public String getAddmissionType() {
        return addmissionType;
    }

    public void setAddmissionType(String addmissionType) {
        this.addmissionType = addmissionType;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getRelativeWeight() {
        return relativeWeight;
    }

    public void setRelativeWeight(Double relativeWeight) {
        this.relativeWeight = relativeWeight;
    }

    public Double getHcpcsPortion() {
        return hcpcsPortion;
    }

    public void setHcpcsPortion(Double hcpcsPortion) {
        this.hcpcsPortion = hcpcsPortion;
    }

    public Double getDrugPortion() {
        return drugPortion;
    }

    public void setDrugPortion(Double drugPortion) {
        this.drugPortion = drugPortion;
    }

    public Double getSurgeryPortion() {
        return surgeryPortion;
    }

    public void setSurgeryPortion(Double surgeryPortion) {
        this.surgeryPortion = surgeryPortion;
    }

    public Double getaLos() {
        return aLos;
    }

    public void setaLos(Double aLos) {
        this.aLos = aLos;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
