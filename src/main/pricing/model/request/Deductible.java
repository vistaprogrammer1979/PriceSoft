/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author smutlak
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "id",
    "type",
    "outNet",
    "value",
    "ceiling",
    "preAuth"
})

@Entity
@Table(name = "PatientInsuranceDeductible")

public class Deductible implements Serializable{
    
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    private Long id;
    
    @Size(max = 200)
    @Column(name = "type", length = 200)
    private String type;
    
    @Column(name = "OutNet")
    @XmlElement(name = "OutNet", required = false)
    private int outNet;
    
    @Column(name = "Value")
    @XmlElement(name = "Value", required = false)
    private Double value;
    
    @Column(name = "Ceiling")
    @XmlElement(name = "Ceiling", required = false)
    private Double ceiling;
    
    @Column(name = "PreAuth")
    @XmlElement(name = "PreAuth", required = false)
    private String preAuth;
    
    @Column(name = "ClaimTotal")
    @XmlTransient
    private Double claimTotal;
    
    
    @JoinColumn(name = "patientInsurance", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private PatientInsurance patientInsurance;

    public Deductible() {
        claimTotal=0.0d;
    }

    public Deductible(Long id) {
        this.id = id;
    }

    public Deductible(Long id, String type, int outNet, Double value, Double ceiling, PatientInsurance patientInsurance) {
        this.id = id;
        this.type = type;
        this.outNet = outNet;
        this.value = value;
        this.ceiling = ceiling;
        this.patientInsurance = patientInsurance;
    }

    
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getOutNet() {
        return outNet;
    }

    public void setOutNet(int outNet) {
        this.outNet = outNet;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Double getCeiling() {
        return ceiling;
    }

    public void setCeiling(Double ceiling) {
        this.ceiling = ceiling;
    }

    public PatientInsurance getPatientInsurance() {
        return patientInsurance;
    }

    public void setPatientInsurance(PatientInsurance patientInsurance) {
        this.patientInsurance = patientInsurance;
    }

    public Double getClaimTotal() {
        return claimTotal;
    }

    public void setClaimTotal(Double claimTotal) {
        this.claimTotal = claimTotal;
    }

    public String getPreAuth() {
        return preAuth;
    }

    public void setPreAuth(String preAuth) {
        this.preAuth = preAuth;
    }
    
        
}
