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
    "start",
    "end",
    "verbal",
    "outcome"
})
@Entity
@Table(name = "Authorisation")
@NamedQueries({
    @NamedQuery(name = "Authorisation.findAll", query = "SELECT a FROM Authorisation a"),
    @NamedQuery(name = "Authorisation.findByStart", query = "SELECT a FROM Authorisation a WHERE a.start = :start"),
    @NamedQuery(name = "Authorisation.findByEnd", query = "SELECT a FROM Authorisation a WHERE a.end = :end"),
    @NamedQuery(name = "Authorisation.findByVerbal", query = "SELECT a FROM Authorisation a WHERE a.verbal = :verbal"),
    @NamedQuery(name = "Authorisation.findById", query = "SELECT a FROM Authorisation a WHERE a.id = :id")})
public class Authorisation implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;

    @Size(max = 200)
    @Column(name = "StartDate")
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "Start", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date start;

    @Size(max = 200)
    @Column(name = "EndDate", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "End", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date end;

    @Column(name = "Verbal")
    @XmlElement(name = "Verbal", required = true)
    private Boolean verbal;

    @JoinColumn(name = "EncounterID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Encounter encounterID;

    @OneToMany(mappedBy = "authorisation", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<AuthorisationOutcome> outcome;

    public Authorisation() {
    }

    public Authorisation(Integer id) {
        this.id = id;
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

    public Boolean getVerbal() {
        return verbal;
    }

    public void setVerbal(Boolean verbal) {
        this.verbal = verbal;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Encounter getEncounterID() {
        return encounterID;
    }

    public void setEncounterID(Encounter encounterID) {
        this.encounterID = encounterID;
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
//        if (!(object instanceof Authorisation)) {
//            return false;
//        }
//        Authorisation other = (Authorisation) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Authorisation[ id=" + id + " ]";
//    }

    public List<AuthorisationOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<AuthorisationOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new AuthorisationOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<AuthorisationOutcome>();
            this.outcome.add(new AuthorisationOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
