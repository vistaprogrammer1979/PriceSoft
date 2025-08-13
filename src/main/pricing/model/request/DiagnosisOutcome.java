/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "id",
    "ruleName",
    "ruleID",
    "severity",
    "shortMsg",
    "longMsg"
})
@Entity
@Table(name = "DiagnosisOutcome")

@NamedQueries({
    @NamedQuery(name = "RequestOutcome.findAll", query = "SELECT r FROM RequestOutcome r"),
    @NamedQuery(name = "RequestOutcome.findByRuleName", query = "SELECT r FROM RequestOutcome r WHERE r.ruleName = :ruleName"),
    @NamedQuery(name = "RequestOutcome.findByRuleID", query = "SELECT r FROM RequestOutcome r WHERE r.ruleID = :ruleID"),
    @NamedQuery(name = "RequestOutcome.findBySeverity", query = "SELECT r FROM RequestOutcome r WHERE r.severity = :severity"),
    @NamedQuery(name = "RequestOutcome.findById", query = "SELECT r FROM RequestOutcome r WHERE r.id = :id")})
public class DiagnosisOutcome implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    private Integer id;

    @Size(max = 200)
    @Column(name = "RuleName", length = 200)
    private String ruleName;
    @Size(max = 200)
    @Column(name = "RuleID", length = 200)
    private String ruleID;
    @Size(max = 200)
    @Column(name = "Severity", length = 200)
    private String severity;
    @Lob
    @Size(max = 2147483647)
    @Column(name = "LongMsg", length = 2147483647)
    private String longMsg;
    @Lob
    @Size(max = 2147483647)
    @Column(name = "ShortMsg", length = 2147483647)
    private String shortMsg;

    @JoinColumn(name = "diagnosis", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Diagnosis diagnosis;

    public DiagnosisOutcome() {
    }

    public DiagnosisOutcome(Integer id) {
        this.id = id;
    }
    
    public DiagnosisOutcome(String severity, String ruleName, String shortMsg, String longMsg) {
        if (shortMsg.equalsIgnoreCase(longMsg)) {
            longMsg = "E";
        }
        this.ruleName = ruleName;
        this.severity = severity;
        this.longMsg = longMsg;
        this.shortMsg = shortMsg;
        ruleID = "";
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleID() {
        return ruleID;
    }

    public void setRuleID(String ruleID) {
        this.ruleID = ruleID;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getLongMsg() {
        return longMsg;
    }

    public void setLongMsg(String longMsg) {
        this.longMsg = longMsg;
    }

    public String getShortMsg() {
        return shortMsg;
    }

    public void setShortMsg(String shortMsg) {
        this.shortMsg = shortMsg;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Diagnosis getDiagnosis() {
        return diagnosis;
    }

    public void setDiagnosis(Diagnosis diagnosis) {
        this.diagnosis = diagnosis;
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
//        if (!(object instanceof DiagnosisOutcome)) {
//            return false;
//        }
//        DiagnosisOutcome other = (DiagnosisOutcome) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.response.RequestOutcome[ id=" + id + " ]";
//    }

//    public com.accumed.model.batch.outcome.Outcome convert(){
//        return new com.accumed.model.batch.outcome.Outcome(severity, ruleName, shortMsg, longMsg);
//    }
}
