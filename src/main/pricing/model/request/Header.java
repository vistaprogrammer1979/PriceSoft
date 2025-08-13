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
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "workflow",
    "validationType",
    "extendedValidationType",
    "server",
    "callingApplication",
    "callingApplicationVersion",
    "userID",
    "userName",
    "senderID",
    "receiverID",
    "transactionDate",
    "batchStartDate",
    "batchEndDate",
    "recordCount",
    "dispositionFlag",
    "top20",
    "outcome"
})

@Entity
@Table(name = "Header")
@NamedQueries({
    @NamedQuery(name = "Header.findAll", query = "SELECT h FROM Header h"),
    @NamedQuery(name = "Header.findByExtendedValidationType", query = "SELECT h FROM Header h WHERE h.extendedValidationType = :extendedValidationType"),
    @NamedQuery(name = "Header.findByServer", query = "SELECT h FROM Header h WHERE h.server = :server"),
    @NamedQuery(name = "Header.findByCallingApplication", query = "SELECT h FROM Header h WHERE h.callingApplication = :callingApplication"),
    @NamedQuery(name = "Header.findByCallingApplicationVersion", query = "SELECT h FROM Header h WHERE h.callingApplicationVersion = :callingApplicationVersion"),
    @NamedQuery(name = "Header.findByUserID", query = "SELECT h FROM Header h WHERE h.userID = :userID"),
    @NamedQuery(name = "Header.findByUserName", query = "SELECT h FROM Header h WHERE h.userName = :userName"),
    @NamedQuery(name = "Header.findByRequestID", query = "SELECT h FROM Header h WHERE h.requestID = :requestID"),
    @NamedQuery(name = "Header.findBySenderID", query = "SELECT h FROM Header h WHERE h.senderID = :senderID"),
    @NamedQuery(name = "Header.findByReceiverID", query = "SELECT h FROM Header h WHERE h.receiverID = :receiverID"),
    @NamedQuery(name = "Header.findByTransactionDate", query = "SELECT h FROM Header h WHERE h.transactionDate = :transactionDate"),
    @NamedQuery(name = "Header.findByBatchStartDate", query = "SELECT h FROM Header h WHERE h.batchStartDate = :batchStartDate"),
    @NamedQuery(name = "Header.findByBatchEndDate", query = "SELECT h FROM Header h WHERE h.batchEndDate = :batchEndDate"),
    @NamedQuery(name = "Header.findByRecordCount", query = "SELECT h FROM Header h WHERE h.recordCount = :recordCount"),
    @NamedQuery(name = "Header.findByDispositionFlag", query = "SELECT h FROM Header h WHERE h.dispositionFlag = :dispositionFlag"),
    @NamedQuery(name = "Header.findById", query = "SELECT h FROM Header h WHERE h.id = :id")})
public class Header implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;

    @Size(max = 200)
    @Column(name = "Server", length = 200)
    @XmlElement(name = "Server", required = false)
    private String server;
    @Size(max = 200)
    @Column(name = "CallingApplication", length = 200)
    @XmlElement(name = "CallingApplication", required = false)
    private String callingApplication;
    @Size(max = 200)
    @Column(name = "CallingApplicationVersion", length = 200)
    @XmlElement(name = "CallingApplicationVersion", required = false)
    private String callingApplicationVersion;
    @Column(name = "UserID")
    @XmlElement(name = "UserID")
    private Integer userID;
    @Size(max = 200)
    @Column(name = "UserName", length = 200)
    @XmlElement(name = "UserName", required = false)
    private String userName;

    @Size(max = 200)
    @Column(name = "SenderID", length = 200)
    @XmlElement(name = "SenderID", required = true)
    private String senderID;
    @Size(max = 200)
    @Column(name = "ReceiverID", length = 200)
    @XmlElement(name = "ReceiverID", required = true)
    private String receiverID;
    @Size(max = 200)
    @Column(name = "TransactionDate", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "TransactionDate", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date transactionDate;
    @Size(max = 200)
    @Column(name = "BatchStartDate", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "BatchStartDate", required = false)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date batchStartDate;
    @Size(max = 200)
    @Column(name = "BatchEndDate", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "BatchEndDate", required = false)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date batchEndDate;
    @Column(name = "RecordCount")
    @XmlElement(name = "RecordCount", required = true)
    @XmlSchemaType(name = "nonNegativeInteger")
    private Integer recordCount;
    @Size(max = 200)
    @Column(name = "DispositionFlag", length = 200)
    @XmlElement(name = "DispositionFlag", required = true)
    private String dispositionFlag;

    @Column(name = "top20")
    @XmlElement(name = "top20", required = false)
    private Integer top20;

    @OneToOne(mappedBy = "headerID", cascade = CascadeType.PERSIST)
    @XmlElement(name = "Workflow", required = false)
    private Workflow workflow;

    @OneToMany(mappedBy = "headerID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "ExtendedValidationType")
    private List<ExtendedValidationType> extendedValidationType;

    @Size(max = 200)
    @Column(name = "ValidationType", length = 200)
    @XmlElement(name = "ValidationType")
    private String validationType;

    @JoinColumn(name = "requestID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Request requestID;

    @OneToMany(mappedBy = "header", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<HeaderOutcome> outcome;

    public Header() {
    }

    public Header(Integer id) {
        this.id = id;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getCallingApplication() {
        return callingApplication;
    }

    public void setCallingApplication(String callingApplication) {
        this.callingApplication = callingApplication;
    }

    public String getCallingApplicationVersion() {
        return callingApplicationVersion;
    }

    public void setCallingApplicationVersion(String callingApplicationVersion) {
        this.callingApplicationVersion = callingApplicationVersion;
    }

    public Integer getUserID() {
        return userID;
    }

    public void setUserID(Integer userID) {
        this.userID = userID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Request getRequestID() {
        return requestID;
    }

    public void setRequestID(Request requestID) {
        this.requestID = requestID;
    }

    public String getSenderID() {
        return senderID;
    }

    public void setSenderID(String senderID) {
        this.senderID = senderID;
    }

    public String getReceiverID() {
        return receiverID;
    }

    public void setReceiverID(String receiverID) {
        this.receiverID = receiverID;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(Date transactionDate) {
        this.transactionDate = transactionDate;
    }

    public Date getBatchStartDate() {
        return batchStartDate;
    }

    public void setBatchStartDate(Date batchStartDate) {
        this.batchStartDate = batchStartDate;
    }

    public Date getBatchEndDate() {
        return batchEndDate;
    }

    public void setBatchEndDate(Date batchEndDate) {
        this.batchEndDate = batchEndDate;
    }

    public String getValidationType() {
        return validationType;
    }

    public void setValidationType(String validationType) {
        this.validationType = validationType;
    }

    public List<ExtendedValidationType> getExtendedValidationType() {
        return extendedValidationType;
    }

    public void setExtendedValidationType(List<ExtendedValidationType> extendedValidationType) {
        this.extendedValidationType = extendedValidationType;
    }

    public Integer getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(Integer recordCount) {
        this.recordCount = recordCount;
    }

    public String getDispositionFlag() {
        return dispositionFlag;
    }

    public void setDispositionFlag(String dispositionFlag) {
        this.dispositionFlag = dispositionFlag;
    }

    public Workflow getWorkflow() {
        return workflow;
    }

    public void setWorkflow(Workflow workflow) {
        this.workflow = workflow;
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
//        if (!(object instanceof Header)) {
//            return false;
//        }
//        Header other = (Header) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Header[ id=" + id + " ]";
//    }

    public Integer getTop20() {
        return top20;
    }

    public void setTop20(Integer top20) {
        this.top20 = top20;
    }

    public List<HeaderOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<HeaderOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new HeaderOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<HeaderOutcome>();
            this.outcome.add(new HeaderOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
