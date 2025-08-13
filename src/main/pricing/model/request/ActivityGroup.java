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
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
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
    "id",
    "type",
    "name"
})

@Entity
@Table(name = "ActivityGroup")

@NamedQueries({
    @NamedQuery(name = "ActivityGroup.findAll", query = "SELECT o FROM ActivityGroup o")/*,
    @NamedQuery(name = "ActivityGroup.findByType", query = "SELECT o FROM ActivityGroup o WHERE o.type = :type"),
    @NamedQuery(name = "ActivityGroup.findByCode", query = "SELECT o FROM ActivityGroup o WHERE o.code = :code"),
    @NamedQuery(name = "ActivityGroup.findByValue", query = "SELECT o FROM ActivityGroup o WHERE o.value = :value"),
    @NamedQuery(name = "ActivityGroup.findByValueType", query = "SELECT o FROM ActivityGroup o WHERE o.valueType = :valueType"),
    @NamedQuery(name = "ActivityGroup.findById", query = "SELECT o FROM ActivityGroup o WHERE o.id = :id")*/})
public class ActivityGroup implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlElement(name = "ID")
    private Integer id;
    
    
    @Column(name = "Type")
    @XmlElement(name = "Type", required = true)
    private Integer type;

    @Size(max = 1024)
    @Column(name = "Name", length = 1024)
    @XmlElement(name = "Name")
    private String name;


    @JoinColumn(name = "activityID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Activity activityID;


    public ActivityGroup() {
    }

    public ActivityGroup(Integer id) {
        this.id = id;
    }

    public ActivityGroup(Integer id, Integer type, String name, Activity activityID) {
        this.id = id;
        this.type = type;
        this.name = name;
        this.activityID = activityID;
    }
    
    

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
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
//        if (!(object instanceof ActivityGroup)) {
//            return false;
//        }
//        ActivityGroup other = (ActivityGroup) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.ActivityGroup[ id=" + id + " ]";
//    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

       
}
