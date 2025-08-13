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
    "type",
    "outcome"
})

@Entity
@Table(name = "ExtendedValidationType")
@NamedQueries({
    @NamedQuery(name = "ExtendedValidationType.findAll", query = "SELECT a FROM ExtendedValidationType a"),
    @NamedQuery(name = "ExtendedValidationType.findByType", query = "SELECT a FROM ExtendedValidationType a WHERE a.type = :type"),
    @NamedQuery(name = "ExtendedValidationType.findById", query = "SELECT a FROM ExtendedValidationType a WHERE a.id = :id")})
public class ExtendedValidationType implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;
    @Size(max = 200)
    @Column(name = "type", length = 200)
    @XmlElement(name = "Type", required = true)
    private String type;

    @JoinColumn(name = "headerID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Header headerID;

    @OneToMany(mappedBy = "extendedValidationType", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ExtendedValidationTypeOutcome> outcome;

    public ExtendedValidationType() {
    }

    public ExtendedValidationType(String type) {
        this.type = type;
    }

    public ExtendedValidationType(Integer id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Header getHeaderID() {
        return headerID;
    }

    public void setHeaderID(Header headerID) {
        this.headerID = headerID;
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
//        if (!(object instanceof ExtendedValidationType)) {
//            return false;
//        }
//        ExtendedValidationType other = (ExtendedValidationType) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.ExtendedValidationType[ id=" + id + " ]";
//    }

    public List<ExtendedValidationTypeOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ExtendedValidationTypeOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new ExtendedValidationTypeOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ExtendedValidationTypeOutcome>();
            this.outcome.add(new ExtendedValidationTypeOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
