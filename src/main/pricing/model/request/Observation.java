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
    "value",
    "valueType",
    "outcome"
})
@Entity
@Table(name = "Observation")

@NamedQueries({
    @NamedQuery(name = "Observation.findAll", query = "SELECT o FROM Observation o"),
    @NamedQuery(name = "Observation.findByType", query = "SELECT o FROM Observation o WHERE o.type = :type"),
    @NamedQuery(name = "Observation.findByCode", query = "SELECT o FROM Observation o WHERE o.code = :code"),
    @NamedQuery(name = "Observation.findByValue", query = "SELECT o FROM Observation o WHERE o.value = :value"),
    @NamedQuery(name = "Observation.findByValueType", query = "SELECT o FROM Observation o WHERE o.valueType = :valueType"),
    @NamedQuery(name = "Observation.findById", query = "SELECT o FROM Observation o WHERE o.id = :id")})
public class Observation implements Serializable {

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

    @Size(max = 1024)
    @Column(name = "Value", length = 1024)
    @XmlElement(name = "Value")
    private String value;

    @Size(max = 200)
    @Column(name = "ValueType", length = 200)
    @XmlElement(name = "ValueType")
    private String valueType;

    @JoinColumn(name = "activityID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Activity activityID;

    @OneToMany(mappedBy = "observation", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ObservationOutcome> outcome;

    public Observation() {
    }

    public Observation(Integer id) {
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Activity getActivityID() {
        return activityID;
    }

    public void setActivityID(Activity activityID) {
        this.activityID = activityID;
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
//        if (!(object instanceof Observation)) {
//            return false;
//        }
//        Observation other = (Observation) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Observation[ id=" + id + " ]";
//    }

    public List<ObservationOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ObservationOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new ObservationOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ObservationOutcome>();
            this.outcome.add(new ObservationOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }
    
    
}
