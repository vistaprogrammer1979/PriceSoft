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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
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
    "patientName",
    "patientSurname",
    "genderId",
    "emiratesId",
    "passportId",
    "drivingLicense",
    "dateOfBirth",
    "nationality",
    "martialStatus",
    "emirateTypes",
    "patientInsurance",
    "outcome"
})
@Entity
@Table(name = "Patient")

@NamedQueries({
    @NamedQuery(name = "Patient.findAll", query = "SELECT p FROM Patient p"),
    @NamedQuery(name = "Patient.findByPatientName", query = "SELECT p FROM Patient p WHERE p.patientName = :patientName"),
    @NamedQuery(name = "Patient.findByPatientSurname", query = "SELECT p FROM Patient p WHERE p.patientSurname = :patientSurname"),
    @NamedQuery(name = "Patient.findByGenderId", query = "SELECT p FROM Patient p WHERE p.genderId = :genderId"),
    @NamedQuery(name = "Patient.findByEmiratesId", query = "SELECT p FROM Patient p WHERE p.emiratesId = :emiratesId"),
    @NamedQuery(name = "Patient.findByPassportId", query = "SELECT p FROM Patient p WHERE p.passportId = :passportId"),
    @NamedQuery(name = "Patient.findByDrivingLicense", query = "SELECT p FROM Patient p WHERE p.drivingLicense = :drivingLicense"),
    @NamedQuery(name = "Patient.findByDateOfBirth", query = "SELECT p FROM Patient p WHERE p.dateOfBirth = :dateOfBirth"),
    @NamedQuery(name = "Patient.findByNationality", query = "SELECT p FROM Patient p WHERE p.nationality = :nationality"),
    @NamedQuery(name = "Patient.findByMartialStatus", query = "SELECT p FROM Patient p WHERE p.martialStatus = :martialStatus"),
    @NamedQuery(name = "Patient.findByEmirateTypes", query = "SELECT p FROM Patient p WHERE p.emirateTypes = :emirateTypes"),
    @NamedQuery(name = "Patient.findById", query = "SELECT p FROM Patient p WHERE p.id = :id")})
public class Patient implements Serializable {

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
    @Column(name = "PATIENT_NAME", length = 200)
    @XmlElement(name = "PATIENT_NAME")
    private String patientName;

    @Size(max = 200)
    @Column(name = "PATIENT_SURNAME", length = 200)
    @XmlElement(name = "PATIENT_SURNAME")
    private String patientSurname;

    @Size(max = 200)
    @Column(name = "GENDER_ID", length = 200)
    @XmlElement(name = "GENDER_ID", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlGenderAdapter.class)
    private String genderId;

    @Size(max = 200)
    @Column(name = "EMIRATES_ID", length = 200)
    @XmlElement(name = "EMIRATES_ID")
    private String emiratesId;

    @Size(max = 200)
    @Column(name = "PASSPORT_ID", length = 200)
    @XmlElement(name = "PASSPORT_ID")
    private String passportId;

    @Size(max = 200)
    @Column(name = "DRIVING_LICENSE", length = 200)
    @XmlElement(name = "DRIVING_LICENSE")
    private String drivingLicense;

    @Size(max = 200)
    @Column(name = "DATE_OF_BIRTH", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "DATE_OF_BIRTH", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date dateOfBirth;

    @Size(max = 200)
    @Column(name = "NATIONALITY", length = 200)
    @XmlElement(name = "NATIONALITY")
    private String nationality;

    @Size(max = 200)
    @Column(name = "MARTIAL_STATUS", length = 200)
    @XmlElement(name = "MARTIAL_STATUS")
    private String martialStatus;

    @Column(name = "EMIRATE_TYPES")
    @XmlElement(name = "EMIRATE_TYPES", required = true)
    private Integer emirateTypes;

    @OneToOne(mappedBy = "patientID", cascade = CascadeType.PERSIST)
    @XmlElement(name = "PatientInsurance")
    private PatientInsurance patientInsurance;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Claim claimID;

    @OneToMany(mappedBy = "patient", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<PatientOutcome> outcome;

    public Patient() {
    }

    public Patient(Integer id) {
        this.id = id;
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public String getPatientName() {
        return patientName;
    }

    public void setPatientName(String patientName) {
        this.patientName = patientName;
    }

    public String getPatientSurname() {
        return patientSurname;
    }

    public void setPatientSurname(String patientSurname) {
        this.patientSurname = patientSurname;
    }

    public String getGenderId() {
        return genderId;
    }

    public void setGenderId(String genderId) {
        this.genderId = genderId;
    }

    public String getEmiratesId() {
        return emiratesId;
    }

    public void setEmiratesId(String emiratesId) {
        this.emiratesId = emiratesId;
    }

    public String getPassportId() {
        return passportId;
    }

    public void setPassportId(String passportId) {
        this.passportId = passportId;
    }

    public String getDrivingLicense() {
        return drivingLicense;
    }

    public void setDrivingLicense(String drivingLicense) {
        this.drivingLicense = drivingLicense;
    }

    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public String getNationality() {
        return nationality;
    }

    public void setNationality(String nationality) {
        this.nationality = nationality;
    }

    public String getMartialStatus() {
        return martialStatus;
    }

    public void setMartialStatus(String martialStatus) {
        this.martialStatus = martialStatus;
    }

    public Integer getEmirateTypes() {
        return emirateTypes;
    }

    public void setEmirateTypes(Integer emirateTypes) {
        this.emirateTypes = emirateTypes;
    }

    public PatientInsurance getPatientInsurance() {
        return patientInsurance;
    }

    public void setPatientInsurance(PatientInsurance patientInsurance) {
        this.patientInsurance = patientInsurance;
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
//        if (!(object instanceof Patient)) {
//            return false;
//        }
//        Patient other = (Patient) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Patient[ id=" + id + " ]";
//    }

    public List<PatientOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<PatientOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new PatientOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<PatientOutcome>();
            this.outcome.add(new PatientOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
