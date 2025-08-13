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
    "type"
})
@Entity
@Table(name = "ClaimType")

@NamedQueries({
    @NamedQuery(name = "ClaimType.findAll", query = "SELECT d FROM ClaimType d"),
    @NamedQuery(name = "ClaimType.findByType", query = "SELECT d FROM ClaimType d WHERE d.type = :type"),
    @NamedQuery(name = "ClaimType.findById", query = "SELECT d FROM ClaimType d WHERE d.id = :id")})
public class ClaimType implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public static final String Maternity = "Maternity";
    public static final String Dental = "Dental";
    public static final String Optical = "Optical";
    public static final String None = "None";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;
    
    @Size(max = 200)
    @Column(name = "Type", length = 200)
    @XmlElement(name = "Type", required = true)
    private String type;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Claim claimID;

    public ClaimType() {
    }

    public ClaimType(String type) {
        this.type = type;
    }
    
    public ClaimType(Integer id) {
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

    public Claim getClaimID() {
        return claimID;
    }

    public void setClaimID(Claim claimID) {
        this.claimID = claimID;
    }
}
