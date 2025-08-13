/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
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
    "idCaller",
    "type",
    "code",
    "providerType",
    "providerCode",
    "outcome"
})
@Entity
@Table(name = "Diagnosis")

@NamedQueries({
    @NamedQuery(name = "Diagnosis.findAll", query = "SELECT d FROM Diagnosis d"),
    @NamedQuery(name = "Diagnosis.findByType", query = "SELECT d FROM Diagnosis d WHERE d.type = :type"),
    @NamedQuery(name = "Diagnosis.findByCode", query = "SELECT d FROM Diagnosis d WHERE d.code = :code"),
    @NamedQuery(name = "Diagnosis.findById", query = "SELECT d FROM Diagnosis d WHERE d.id = :id")})
public class Diagnosis implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;
    
    @Column(name = "idCaller")
    @XmlElement(name = "idCaller", required = false)
    private Integer idCaller;

    @Size(max = 200)
    @Column(name = "Type", length = 200)
    @XmlElement(name = "Type", required = true)
    private String type;

    @Size(max = 200)
    @Column(name = "Code", length = 200)
    @XmlElement(name = "Code", required = true)
    private String code;
    
    @Size(max = 200)
    @Column(name = "providerType", length = 200)
    @XmlElement(name = "providerType", required = false)
    private String providerType;

    @Size(max = 200)
    @Column(name = "providerCode", length = 200)
    @XmlElement(name = "providerCode", required = false)
    private String providerCode;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Claim claimID;

    @OneToMany(mappedBy = "diagnosis", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<DiagnosisOutcome> outcome;

    public Diagnosis() {
    }

    public Diagnosis(Integer id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Claim getClaimID() {
        return claimID;
    }

    public void setClaimID(Claim claimID) {
        this.claimID = claimID;
    }
//
//    @Override
//    public int hashCode() {
//        int hash = 0;
//        hash += (id != null ? id.hashCode() : 0);
//        return hash;
//    }
//
//    @Override
//    public boolean equals(Object object) {
//        // TODO: Warning - this method won't work in the case the id fields are not set
//        if (!(object instanceof Diagnosis)) {
//            return false;
//        }
//        Diagnosis other = (Diagnosis) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Diagnosis[ id=" + id + " ]";
//    }

    public List<DiagnosisOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<DiagnosisOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new DiagnosisOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<DiagnosisOutcome>();
            this.outcome.add(new DiagnosisOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public String getProviderType() {
        return providerType;
    }

    public void setProviderType(String providerType) {
        this.providerType = providerType;
    }

    public String getProviderCode() {
        return providerCode;
    }

    public void setProviderCode(String providerCode) {
        this.providerCode = providerCode;
    }
    
    
}
