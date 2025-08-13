/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.io.Serializable;
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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "idCaller",
    "patientInsuranceId",
    "relationTo",
    "principleInsuranceId",
    "insuranceLisence",
    "packageName",
    "networkName",
    "networkId",
    "subNetworkName",
    "subNetworkId",
    "planName",
    "planId",
    "startDate",
    "renewalDate",
    "endDate",
    "grossPremium",
    "policyHolderTypeId",
    "principleInsuranceNumber",
    "isExpired",
    "policyNumber",
    "verified",
    "admissionType",
    "isPending",
    "maternity",
    "dental",
    "optical",
    "coPayment",
    "deductible",
    "outcome"
})
@Entity
@Table(name = "PatientInsurance")

@NamedQueries({
    @NamedQuery(name = "PatientInsurance.findAll", query = "SELECT p FROM PatientInsurance p"),
    @NamedQuery(name = "PatientInsurance.findByPatientInsuranceId", query = "SELECT p FROM PatientInsurance p WHERE p.patientInsuranceId = :patientInsuranceId"),
    @NamedQuery(name = "PatientInsurance.findByRelationTo", query = "SELECT p FROM PatientInsurance p WHERE p.relationTo = :relationTo"),
    @NamedQuery(name = "PatientInsurance.findByPrincipleInsuranceId", query = "SELECT p FROM PatientInsurance p WHERE p.principleInsuranceId = :principleInsuranceId"),
    @NamedQuery(name = "PatientInsurance.findByInsuranceLisence", query = "SELECT p FROM PatientInsurance p WHERE p.insuranceLisence = :insuranceLisence"),
    @NamedQuery(name = "PatientInsurance.findByPackageName", query = "SELECT p FROM PatientInsurance p WHERE p.packageName = :packageName"),
    @NamedQuery(name = "PatientInsurance.findByStartDate", query = "SELECT p FROM PatientInsurance p WHERE p.startDate = :startDate"),
    @NamedQuery(name = "PatientInsurance.findByRenewalDate", query = "SELECT p FROM PatientInsurance p WHERE p.renewalDate = :renewalDate"),
    @NamedQuery(name = "PatientInsurance.findByEndDate", query = "SELECT p FROM PatientInsurance p WHERE p.endDate = :endDate"),
    @NamedQuery(name = "PatientInsurance.findByGrossPremium", query = "SELECT p FROM PatientInsurance p WHERE p.grossPremium = :grossPremium"),
    @NamedQuery(name = "PatientInsurance.findByPolicyHolderTypeId", query = "SELECT p FROM PatientInsurance p WHERE p.policyHolderTypeId = :policyHolderTypeId"),
    @NamedQuery(name = "PatientInsurance.findByPrincipleInsuranceNumber", query = "SELECT p FROM PatientInsurance p WHERE p.principleInsuranceNumber = :principleInsuranceNumber"),
    @NamedQuery(name = "PatientInsurance.findByIsExpired", query = "SELECT p FROM PatientInsurance p WHERE p.isExpired = :isExpired"),
    @NamedQuery(name = "PatientInsurance.findByPolicyNumber", query = "SELECT p FROM PatientInsurance p WHERE p.policyNumber = :policyNumber"),
    @NamedQuery(name = "PatientInsurance.findByAdmissionType", query = "SELECT p FROM PatientInsurance p WHERE p.admissionType = :admissionType"),
    @NamedQuery(name = "PatientInsurance.findByIsPending", query = "SELECT p FROM PatientInsurance p WHERE p.isPending = :isPending"),
    @NamedQuery(name = "PatientInsurance.findById", query = "SELECT p FROM PatientInsurance p WHERE p.id = :id")})
public class PatientInsurance implements Serializable {

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
    @Column(name = "PATIENT_INSURANCE_ID", length = 200)
    @XmlElement(name = "PATIENT_INSURANCE_ID", required = true)
    private String patientInsuranceId;

    @Size(max = 200)
    @Column(name = "RELATION_TO", length = 200)
    @XmlElement(name = "RELATION_TO", required = true)
    private String relationTo;

    @Size(max = 200)
    @Column(name = "PRINCIPLE_INSURANCE_ID", length = 200)
    @XmlElement(name = "PRINCIPLE_INSURANCE_ID", required = false)
    private String principleInsuranceId;

    @Size(max = 200)
    @Column(name = "INSURANCE_LISENCE", length = 200)
    @XmlElement(name = "INSURANCE_LISENCE", required = true)
    private String insuranceLisence;

    @Size(max = 200)
    @Column(name = "PACKAGE_NAME", length = 200)
    @XmlElement(name = "PACKAGE_NAME", required = true)
    private String packageName;

    @Size(max = 200)
    @Column(name = "NetworkName", length = 200)
    @XmlElement(name = "NetworkName")
    private String networkName;
    
    @Column(name = "NetworkId")
    @XmlElement(name = "NetworkId")
    private Integer networkId;
    
    @Size(max = 200)
    @Column(name = "SubNetworkName", length = 200)
    @XmlElement(name = "SubNetworkName")
    private String subNetworkName;
    
    @Column(name = "SubNetworkId")
    @XmlElement(name = "SubNetworkId")
    private Integer subNetworkId;
    
    @Size(max = 200)
    @Column(name = "PlanName", length = 200)
    @XmlElement(name = "PlanName")
    private String planName;
    
    @Column(name = "PlanId")
    @XmlElement(name = "PlanId")
    private Integer planId;

    @Size(max = 200)
    @Column(name = "START_DATE", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "START_DATE", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date startDate;

    @Size(max = 200)
    @Column(name = "RENEWAL_DATE", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "RENEWAL_DATE")
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date renewalDate;

    @Size(max = 200)
    @Column(name = "END_DATE", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "END_DATE", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date endDate;

    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "GROSS_PREMIUM", precision = 53)
    @XmlElement(name = "GROSS_PREMIUM")
    private float grossPremium;

    @Size(max = 200)
    @Column(name = "POLICY_HOLDER_TYPE_ID", length = 200)
    @XmlElement(name = "POLICY_HOLDER_TYPE_ID")
    private String policyHolderTypeId;

    /*@Column(name = "DEDUCTABLE", precision = 53)
     private float deductable;*/
    @Size(max = 200)
    @Column(name = "PRINCIPLE_INSURANCE_NUMBER", length = 200)
    @XmlElement(name = "PRINCIPLE_INSURANCE_NUMBER")
    private String principleInsuranceNumber;

    @Column(name = "IS_EXPIRED")
    @XmlElement(name = "IS_EXPIRED", required = true)
    private Integer isExpired;

    @Size(max = 200)
    @Column(name = "Policy_Number", length = 200)
    @XmlElement(name = "Policy_Number")
    private String policyNumber;

    @Column(name = "verified")
    @XmlElement(name = "verified", required = true)
    private Integer verified;

    @Column(name = "ADMISSION_TYPE")
    @XmlElement(name = "ADMISSION_TYPE", required = true)
    private Integer admissionType;

    @Column(name = "IS_PENDING")
    @XmlElement(name = "IS_PENDING", required = true)
    private Integer isPending;
    
    @Column(name = "MATERNITY")
    @XmlElement(name = "MATERNITY", required = false)
    private Integer maternity;
    
    @Column(name = "DENTAL")
    @XmlElement(name = "DENTAL", required = false)
    private Integer dental;
    
    @Column(name = "OPTICAL")
    @XmlElement(name = "OPTICAL", required = false)
    private Integer optical;

    @JoinColumn(name = "patientID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Patient patientID;
    
    @OneToMany(mappedBy = "patientInsurance", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "CoPayment", required = false)
    private List<CoPayment> coPayment;
    
    @OneToMany(mappedBy = "patientInsurance", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Deductible", required = false)
    private List<Deductible> deductible;

    @OneToMany(mappedBy = "patientInsurance", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<PatientInsuranceOutcome> outcome;

    public PatientInsurance() {
    }

    public PatientInsurance(Integer id) {
        this.id = id;
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public String getPatientInsuranceId() {
        return patientInsuranceId;
    }

    public void setPatientInsuranceId(String patientInsuranceId) {
        this.patientInsuranceId = patientInsuranceId;
    }

    public String getRelationTo() {
        return relationTo;
    }

    public void setRelationTo(String relationTo) {
        this.relationTo = relationTo;
    }

    public String getPrincipleInsuranceId() {
        return principleInsuranceId;
    }

    public void setPrincipleInsuranceId(String principleInsuranceId) {
        this.principleInsuranceId = principleInsuranceId;
    }

    public String getInsuranceLisence() {
        return insuranceLisence;
    }

    public void setInsuranceLisence(String insuranceLisence) {
        this.insuranceLisence = insuranceLisence;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getNetworkName() {
        return networkName;
    }

    public void setNetworkName(String networkName) {
        this.networkName = networkName;
    }

    public Integer getNetworkId() {
        return networkId;
    }

    public void setNetworkId(Integer networkId) {
        this.networkId = networkId;
    }

    public String getSubNetworkName() {
        return subNetworkName;
    }

    public void setSubNetworkName(String subNetworkName) {
        this.subNetworkName = subNetworkName;
    }

    public Integer getSubNetworkId() {
        return subNetworkId;
    }

    public void setSubNetworkId(Integer subNetworkId) {
        this.subNetworkId = subNetworkId;
    }
    
    

    public String getPlanName() {
        return planName;
    }

    public void setPlanName(String planName) {
        this.planName = planName;
    }

    

    public Integer getPlanId() {
        return planId;
    }

    public void setPlanId(Integer planId) {
        this.planId = planId;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getRenewalDate() {
        return renewalDate;
    }

    public void setRenewalDate(Date renewalDate) {
        this.renewalDate = renewalDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public float getGrossPremium() {
        return grossPremium;
    }

    public void setGrossPremium(float grossPremium) {
        this.grossPremium = grossPremium;
    }

    public String getPolicyHolderTypeId() {
        return policyHolderTypeId;
    }

    public void setPolicyHolderTypeId(String policyHolderTypeId) {
        this.policyHolderTypeId = policyHolderTypeId;
    }

    public String getPrincipleInsuranceNumber() {
        return principleInsuranceNumber;
    }

    public void setPrincipleInsuranceNumber(String principleInsuranceNumber) {
        this.principleInsuranceNumber = principleInsuranceNumber;
    }

    public Integer getIsExpired() {
        return isExpired;
    }

    public void setIsExpired(Integer isExpired) {
        this.isExpired = isExpired;
    }

    public String getPolicyNumber() {
        return policyNumber;
    }

    public void setPolicyNumber(String policyNumber) {
        this.policyNumber = policyNumber;
    }

    public Integer getVerified() {
        return verified;
    }

    public void setVerified(Integer verified) {
        this.verified = verified;
    }

    public Integer getAdmissionType() {
        return admissionType;
    }

    public void setAdmissionType(Integer admissionType) {
        this.admissionType = admissionType;
    }

    public Integer getIsPending() {
        return isPending;
    }

    public void setIsPending(Integer isPending) {
        this.isPending = isPending;
    }

    public Integer getMaternity() {
        return maternity;
    }

    public void setMaternity(Integer maternity) {
        this.maternity = maternity;
    }

    public Integer getDental() {
        return dental;
    }

    public void setDental(Integer dental) {
        this.dental = dental;
    }

    public Integer getOptical() {
        return optical;
    }

    public void setOptical(Integer optical) {
        this.optical = optical;
    }
    
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Patient getPatientID() {
        return patientID;
    }

    public void setPatientID(Patient patientID) {
        this.patientID = patientID;
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
//        if (!(object instanceof PatientInsurance)) {
//            return false;
//        }
//        PatientInsurance other = (PatientInsurance) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.PatientInsurance[ id=" + id + " ]";
//    }

    public List<PatientInsuranceOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<PatientInsuranceOutcome> outcome) {
        this.outcome = outcome;
    }

    public List<CoPayment> getCoPayment() {
        return coPayment;
    }

    public void setCoPayment(List<CoPayment> coPayment) {
        this.coPayment = coPayment;
    }

    

    public List<Deductible> getDeductible() {
        return deductible;
    }

    public void setDeductible(List<Deductible> deductible) {
        this.deductible = deductible;
    }

    
    
}
