/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
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
import javax.xml.bind.annotation.XmlTransient;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "idCaller",
    "facilityID",
    "type",
    "patientID",
    "start",
    "end",
    "startType",
    "endType",
    "transferSource",
    "transferDestination",
    "newPatient",
    "orderingClinician",
    "authorisation",
    "outcome"
})
@Entity
@Table(name = "Encounter")
@NamedQueries({
    @NamedQuery(name = "Encounter.findAll", query = "SELECT e FROM Encounter e"),
    @NamedQuery(name = "Encounter.findByFacilityID", query = "SELECT e FROM Encounter e WHERE e.facilityID = :facilityID"),
    @NamedQuery(name = "Encounter.findByType", query = "SELECT e FROM Encounter e WHERE e.type = :type"),
    @NamedQuery(name = "Encounter.findByPatientID", query = "SELECT e FROM Encounter e WHERE e.patientID = :patientID"),
    @NamedQuery(name = "Encounter.findByStart", query = "SELECT e FROM Encounter e WHERE e.start = :start"),
    @NamedQuery(name = "Encounter.findByEnd", query = "SELECT e FROM Encounter e WHERE e.end = :end"),
    @NamedQuery(name = "Encounter.findByStartType", query = "SELECT e FROM Encounter e WHERE e.startType = :startType"),
    @NamedQuery(name = "Encounter.findByEndType", query = "SELECT e FROM Encounter e WHERE e.endType = :endType"),
    @NamedQuery(name = "Encounter.findByTransferSource", query = "SELECT e FROM Encounter e WHERE e.transferSource = :transferSource"),
    @NamedQuery(name = "Encounter.findByTransferDestination", query = "SELECT e FROM Encounter e WHERE e.transferDestination = :transferDestination"),
    @NamedQuery(name = "Encounter.findByNewPatient", query = "SELECT e FROM Encounter e WHERE e.newPatient = :newPatient"),
    @NamedQuery(name = "Encounter.findById", query = "SELECT e FROM Encounter e WHERE e.id = :id")})
public class Encounter implements Serializable {

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
    @Column(name = "FacilityID", length = 200)
    @XmlElement(name = "FacilityID", required = true)
    private String facilityID;

    @Column(name = "Type")
    @XmlElement(name = "Type", required = true)
    private Integer type;

    @Size(max = 200)
    @Column(name = "PatientID", length = 200)
    @XmlElement(name = "PatientID", required = true)
    private String patientID;

    @Size(max = 200)
    @Column(name = "StartDate")
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "Start", required = false)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date start;

    @Size(max = 200)
    @Column(name = "EndDate")
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "End", required = false)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date end;

    @Column(name = "StartType")
    @XmlElement(name = "StartType")
    private Integer startType;

    @Column(name = "EndType")
    @XmlElement(name = "EndType")
    private Integer endType;

    @Size(max = 200)
    @Column(name = "TransferSource", length = 200)
    @XmlElement(name = "TransferSource")
    private String transferSource;

    @Size(max = 200)
    @Column(name = "TransferDestination", length = 200)
    @XmlElement(name = "TransferDestination")
    private String transferDestination;

    @Column(name = "NewPatient")
    @XmlElement(name = "NewPatient")
    private Integer newPatient;
    
    @Size(max = 200)
    @Column(name = "orderingClinician", length = 200)
    @XmlElement(name = "orderingClinician", required = false)
    private String orderingClinician;

    @OneToOne(mappedBy = "encounterID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Authorisation")
    private Authorisation authorisation;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Claim claimID;

    @OneToMany(mappedBy = "encounter", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<EncounterOutcome> outcome;

    public Encounter() {
    }

    public Encounter(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public String getFacilityID() {
        return facilityID;
    }

    public void setFacilityID(String facilityID) {
        this.facilityID = facilityID;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getPatientID() {
        return patientID;
    }

    public void setPatientID(String patientID) {
        this.patientID = patientID;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    public Integer getStartType() {
        return startType;
    }

    public void setStartType(Integer startType) {
        this.startType = startType;
    }

    public Integer getEndType() {
        return endType;
    }

    public void setEndType(Integer endType) {
        this.endType = endType;
    }

    public String getTransferSource() {
        return transferSource;
    }

    public void setTransferSource(String transferSource) {
        this.transferSource = transferSource;
    }

    public String getTransferDestination() {
        return transferDestination;
    }

    public void setTransferDestination(String transferDestination) {
        this.transferDestination = transferDestination;
    }

    public Integer getNewPatient() {
        return newPatient;
    }

    public void setNewPatient(Integer newPatient) {
        this.newPatient = newPatient;
    }

    public Authorisation getAuthorisation() {
        return authorisation;
    }

    public void setAuthorisation(Authorisation authorisation) {
        this.authorisation = authorisation;
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
//        if (!(object instanceof Encounter)) {
//            return false;
//        }
//        Encounter other = (Encounter) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Encounter[ id=" + id + " ]";
//    }

    public List<EncounterOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<EncounterOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new EncounterOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<EncounterOutcome>();
            this.outcome.add(new EncounterOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    public String getOrderingClinician() {
        return orderingClinician;
    }

    public void setOrderingClinician(String orderingClinician) {
        this.orderingClinician = orderingClinician;
    }
    
    
}
