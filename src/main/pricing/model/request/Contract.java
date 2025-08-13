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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
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
    "packageName",
    "network",
    "subNetworkName",
    "policy",
    "outcome"
})
@Entity
@Table(name = "Contract")
@NamedQueries({
    @NamedQuery(name = "Contract.findAll", query = "SELECT c FROM Contract c"),
    @NamedQuery(name = "Contract.findByPackageName", query = "SELECT c FROM Contract c WHERE c.packageName = :packageName"),
    @NamedQuery(name = "Contract.findByPolicy", query = "SELECT c FROM Contract c WHERE c.policy = :policy"),
    @NamedQuery(name = "Contract.findById", query = "SELECT c FROM Contract c WHERE c.id = :id")})
public class Contract implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;

    @Size(max = 200)
    @Column(name = "PackageName", length = 200)
    @XmlElement(name = "PackageName")
    private String packageName;
    
    @Size(max = 200)
    @Column(name = "Network", length = 200)
    @XmlElement(name = "Network")
    private String network;
    
    @Size(max = 200)
    @Column(name = "SubNetworkName", length = 200)
    @XmlElement(name = "SubNetworkName")
    private String subNetworkName;

    @Size(max = 200)
    @Column(name = "Policy", length = 200)
    @XmlElement(name = "Policy")
    private String policy;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Claim claimID;

    @OneToMany(mappedBy = "contract", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ContractOutcome> outcome;

    public Contract() {
    }

    public Contract(Integer id) {
        this.id = id;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getSubNetworkName() {
        return subNetworkName;
    }

    public void setSubNetworkName(String subNetworkName) {
        this.subNetworkName = subNetworkName;
    }
    
    
    
    public String getPolicy() {
        return policy;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
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
//        if (!(object instanceof Contract)) {
//            return false;
//        }
//        Contract other = (Contract) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Contract[ id=" + id + " ]";
//    }

    public List<ContractOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ContractOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new ContractOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ContractOutcome>();
            this.outcome.add(new ContractOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
