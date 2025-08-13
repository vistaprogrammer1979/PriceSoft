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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlTransient;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author smutlak
 */
@XmlRootElement(name = "Claim")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "rootID",
    "idCaller",
    "SPC_ID",
    "CUS_ID",
    "multipleProcedures",
    "primaryProc",
    "secondaryProc",
    "thirdProc",
    "forthProc",
    "CUS_DENTAL_ID",
    "IDPayer",
    "memberID",
    "payerID",
    "receiverID",
    "providerID",
    "emiratesIDNumber",
    "gross",
    "patientShare",
    "net",
    "appliedDeductible",
    "appliedCopayment",
    "providerInvoiceAmount",
    "denialCode",
    "paymentReference",
    "dateSettlement",
    "pending",
    "imported",
    "encounter",
    "claimType",
    "diagnosis",
    "activity",
    "resubmission",
    "contract",
    "patient",
    "logInfo",
    "outcome"
})
@Entity
@Table(name = "Claim")
@NamedQueries({
    @NamedQuery(name = "Claim.findAll", query = "SELECT c FROM Claim c"),
    @NamedQuery(name = "Claim.findById", query = "SELECT c FROM Claim c WHERE c.id = :id"),
    @NamedQuery(name = "Claim.findByMemberID", query = "SELECT c FROM Claim c WHERE c.memberID = :memberID"),
    @NamedQuery(name = "Claim.findByPayerID", query = "SELECT c FROM Claim c WHERE c.payerID = :payerID"),
    @NamedQuery(name = "Claim.findByProviderID", query = "SELECT c FROM Claim c WHERE c.providerID = :providerID"),
    @NamedQuery(name = "Claim.findByEmiratesIDNumber", query = "SELECT c FROM Claim c WHERE c.emiratesIDNumber = :emiratesIDNumber"),
    @NamedQuery(name = "Claim.findByGross", query = "SELECT c FROM Claim c WHERE c.gross = :gross"),
    @NamedQuery(name = "Claim.findByPatientShare", query = "SELECT c FROM Claim c WHERE c.patientShare = :patientShare"),
    @NamedQuery(name = "Claim.findByNet", query = "SELECT c FROM Claim c WHERE c.net = :net"),
    @NamedQuery(name = "Claim.findByProviderInvoiceAmount", query = "SELECT c FROM Claim c WHERE c.providerInvoiceAmount = :providerInvoiceAmount"),
    @NamedQuery(name = "Claim.findByDenialCode", query = "SELECT c FROM Claim c WHERE c.denialCode = :denialCode"),
    @NamedQuery(name = "Claim.findByPaymentReference", query = "SELECT c FROM Claim c WHERE c.paymentReference = :paymentReference"),
    @NamedQuery(name = "Claim.findByDateSettlement", query = "SELECT c FROM Claim c WHERE c.dateSettlement = :dateSettlement"),
    @NamedQuery(name = "Claim.findByPending", query = "SELECT c FROM Claim c WHERE c.pending = :pending"),
    @NamedQuery(name = "Claim.findByImported", query = "SELECT c FROM Claim c WHERE c.imported = :imported"),
    @NamedQuery(name = "Claim.findById", query = "SELECT c FROM Claim c WHERE c.id = :id")})
public class Claim implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;

    @Column(name = "rootID")
    @XmlElement(name = "ID", required = true)
    private Integer rootID;

    @Column(name = "idCaller")
    @XmlElement(name = "idCaller", required = false)
    private Integer idCaller;
    
    @Column(name = "SPC_ID")
    @XmlElement(name = "SPC_ID", required = false)
    private Integer SPC_ID;
    
    @Column(name = "CUS_ID")
    @XmlElement(name = "CUS_ID", required = false)
    private Integer CUS_ID;
    
    @Column(name = "MultipleProcedures")
    @XmlElement(name = "MultipleProcedures", required = false)
    private Integer multipleProcedures;
    
    @Column(name = "PrimaryProc")
    @XmlElement(name = "PrimaryProc", required = false)
    private Double primaryProc;
    
    @Column(name = "SecondaryProc")
    @XmlElement(name = "SecondaryProc", required = false)
    private Double secondaryProc;
    
    @Column(name = "ThirdProc")
    @XmlElement(name = "ThirdProc", required = false)
    private Double thirdProc;
    
    @Column(name = "ForthProc")
    @XmlElement(name = "ForthProc", required = false)
    private Double forthProc;
    
    @Column(name = "CUS_DENTAL_ID")
    @XmlElement(name = "CUS_DENTAL_ID", required = false)
    private Integer CUS_DENTAL_ID;

    @Size(max = 200)
    @Column(name = "IDPayer", length = 200)
    @XmlElement(name = "IDPayer")
    private String IDPayer;

    @Size(max = 200)
    @Column(name = "MemberID", length = 200)
    @XmlElement(name = "MemberID")
    private String memberID;

    @Size(max = 200)
    @Column(name = "PayerID", length = 200)
    @XmlElement(name = "PayerID", required = true)
    private String payerID;
    
    
    @Size(max = 200)
    @Column(name = "ReceiverID", length = 200)
    @XmlElement(name = "ReceiverID", required = true)
    private String receiverID;

    @Size(max = 200)
    @Column(name = "ProviderID", length = 200)
    @XmlElement(name = "ProviderID", required = true)
    private String providerID;

    @Size(max = 200)
    @Column(name = "EmiratesIDNumber", length = 200)
    @XmlElement(name = "EmiratesIDNumber", required = true)
    private String emiratesIDNumber;
    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "Gross", precision = 53)
    @XmlElement(name = "Gross")
    private Double gross;

    @Column(name = "PatientShare", precision = 53)
    @XmlElement(name = "PatientShare")
    private Double patientShare;

    @Column(name = "Net", precision = 53)
    @XmlElement(name = "Net")
    private Double net;
    
    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "AppliedDeductible", required = false)
    private List<AppliedDeductible> appliedDeductible;
    
    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "AppliedCopayment", required = false)
    private List<AppliedCopayment> appliedCopayment;

    @Column(name = "ProviderInvoiceAmount", precision = 53)
    @XmlElement(name = "ProviderInvoiceAmount")
    private Double providerInvoiceAmount;

    @Size(max = 200)
    @Column(name = "DenialCode", length = 200)
    @XmlElement(name = "DenialCode")
    private String denialCode;

    @Size(max = 200)
    @Column(name = "PaymentReference", length = 200)
    @XmlElement(name = "PaymentReference")
    private String paymentReference;

    @Size(max = 200)
    @Column(name = "DateSettlement", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "DateSettlement", required = false)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date dateSettlement;

    @Column(name = "Pending")
    @XmlElement(name = "Pending")
    private Boolean pending;

    @Column(name = "Imported")
    @XmlElement(name = "Imported")
    private Boolean imported;

    /*@Size(max = 256)
     @Column(name = "routingRule", length = 256)
     @XmlTransient
     private String routingRule;*/
    @JoinColumn(name = "request", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Request request;

    @OneToOne(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Contract")
    private Contract contract;

    @OneToOne(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Resubmission")
    private Resubmission resubmission;

    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Activity", required = true)
    private List<Activity> activity;
    
    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "ClaimType", required = true)
    private List<ClaimType> claimType;

    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Diagnosis", required = true)
    private List<Diagnosis> diagnosis;

    @OneToMany(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Encounter", required = true)
    private List<Encounter> encounter;

    @OneToOne(mappedBy = "claimID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Patient")
    private Patient patient;
    
    @Column(name = "LogInfo")
    @XmlElement(name = "LogInfo")
    private Boolean logInfo;

    @OneToMany(mappedBy = "claim", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ClaimOutcome> outcome;

    public Claim() {
        this.appliedCopayment = new ArrayList();
        this.appliedDeductible = new ArrayList();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getRootID() {
        return rootID;
    }

    public void setRootID(Integer rootID) {
        this.rootID = rootID;
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public Integer getSPC_ID() {
        return SPC_ID;
    }

    public void setSPC_ID(Integer SPC_ID) {
        this.SPC_ID = SPC_ID;
    }

    public Integer getCUS_ID() {
        return CUS_ID;
    }

    public void setCUS_ID(Integer CUS_ID) {
        this.CUS_ID = CUS_ID;
    }

    public Integer getCUS_DENTAL_ID() {
        return CUS_DENTAL_ID;
    }

    public void setCUS_DENTAL_ID(Integer CUS_DENTAL_ID) {
        this.CUS_DENTAL_ID = CUS_DENTAL_ID;
    }

    
    
    public String getIDPayer() {
        return IDPayer;
    }

    public void setIDPayer(String IDPayer) {
        this.IDPayer = IDPayer;
    }

    public String getMemberID() {
        return memberID;
    }

    public void setMemberID(String memberID) {
        this.memberID = memberID;
    }

    public String getPayerID() {
        return payerID;
    }

    public void setPayerID(String payerID) {
        this.payerID = payerID;
    }

    public String getReceiverID() {
        return receiverID;
    }

    public void setReceiverID(String receiverID) {
        this.receiverID = receiverID;
    }

    
    
    public String getProviderID() {
        return providerID;
    }

    public void setProviderID(String providerID) {
        this.providerID = providerID;
    }

    public String getEmiratesIDNumber() {
        return emiratesIDNumber;
    }

    public void setEmiratesIDNumber(String emiratesIDNumber) {
        this.emiratesIDNumber = emiratesIDNumber;
    }

    public Double getGross() {
        return gross;
    }

    public void setGross(Double gross) {
        this.gross = gross;
    }

    public Double getPatientShare() {
        return patientShare;
    }

    public void setPatientShare(Double patientShare) {
        this.patientShare = patientShare;
    }

    public Double getNet() {
        return net;
    }

    public void setNet(Double net) {
        this.net = net;
    }

    public void addAppliedDeductible(AppliedDeductible ded){
        appliedDeductible.add(ded);
    }
    
    public void addAppliedCopayment(AppliedCopayment copay){
        appliedCopayment.add(copay);
    }
    
    public List<AppliedDeductible> getAppliedDeductible() {
        return appliedDeductible;
    }

    public void setAppliedDeductible(List<AppliedDeductible> appliedDeductible) {
        this.appliedDeductible = appliedDeductible;
    }

    public List<AppliedCopayment> getAppliedCopayment() {
        return appliedCopayment;
    }

    public void setAppliedCopayment(List<AppliedCopayment> appliedCopayment) {
        this.appliedCopayment = appliedCopayment;
    }
    
    

    public Double getProviderInvoiceAmount() {
        return providerInvoiceAmount;
    }

    public void setProviderInvoiceAmount(Double providerInvoiceAmount) {
        this.providerInvoiceAmount = providerInvoiceAmount;
    }

    public String getDenialCode() {
        return denialCode;
    }

    public void setDenialCode(String denialCode) {
        this.denialCode = denialCode;
    }

    public String getPaymentReference() {
        return paymentReference;
    }

    public void setPaymentReference(String paymentReference) {
        this.paymentReference = paymentReference;
    }

    public Date getDateSettlement() {
        return dateSettlement;
    }

    public void setDateSettlement(Date dateSettlement) {
        this.dateSettlement = dateSettlement;
    }

    public Boolean getPending() {
        return pending;
    }

    public void setPending(Boolean pending) {
        this.pending = pending;
    }

    public Boolean getImported() {
        return imported;
    }

    public void setImported(Boolean imported) {
        this.imported = imported;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public Contract getContract() {
        return contract;
    }

    public void setContract(Contract contract) {
        this.contract = contract;
    }

    public Resubmission getResubmission() {
        return resubmission;
    }

    public void setResubmission(Resubmission resubmission) {
        this.resubmission = resubmission;
    }

    public List<Activity> getActivity() {
        return activity;
    }

    public void setActivity(List<Activity> activity) {
        this.activity = activity;
    }
    
    public List<ClaimType> getClaimType() {
        return claimType;
    }

    public void addClaimType(ClaimType cType) {
        if(claimType == null){
            claimType = new ArrayList();
        }
        claimType.add(cType);
    }
    
    public void setClaimType(List<ClaimType> claimType) {
        this.claimType = claimType;
    }

    public List<Diagnosis> getDiagnosis() {
        return diagnosis;
    }

    public void setDiagnosis(List<Diagnosis> diagnosis) {
        this.diagnosis = diagnosis;
    }

    public List<Encounter> getEncounter() {
        return encounter;
    }

    public void setEncounter(List<Encounter> encounter) {
        this.encounter = encounter;
    }

    public Patient getPatient() {
        return patient;
    }

    public void setPatient(Patient patient) {
        this.patient = patient;
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
//        if (!(object instanceof Claim)) {
//            return false;
//        }
//        Claim other = (Claim) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Claim[ id=" + id + " ]";
//    }

    public List<ClaimOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ClaimOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new ClaimOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ClaimOutcome>();
            this.outcome.add(new ClaimOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    public Boolean getLogInfo() {
        return logInfo;
    }

    public void setLogInfo(Boolean logInfo) {
        this.logInfo = logInfo;
    }

    public Integer getMultipleProcedures() {
        return multipleProcedures;
    }

    public void setMultipleProcedures(Integer multipleProcedures) {
        this.multipleProcedures = multipleProcedures;
    }

    public Double getPrimaryProc() {
        return primaryProc;
    }

    public void setPrimaryProc(Double primaryProc) {
        this.primaryProc = primaryProc;
    }

    public Double getSecondaryProc() {
        return secondaryProc;
    }

    public void setSecondaryProc(Double secondaryProc) {
        this.secondaryProc = secondaryProc;
    }

    public Double getThirdProc() {
        return thirdProc;
    }

    public void setThirdProc(Double thirdProc) {
        this.thirdProc = thirdProc;
    }

    public Double getForthProc() {
        return forthProc;
    }

    public void setForthProc(Double forthProc) {
        this.forthProc = forthProc;
    }
    
    
}
