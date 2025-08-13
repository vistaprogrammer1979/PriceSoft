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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
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
    "type",
    "comment",
    "attachment",
    "outcome"
})
@Entity
@Table(name = "Resubmission")

@NamedQueries({
    @NamedQuery(name = "Resubmission.findAll", query = "SELECT r FROM Resubmission r"),
    @NamedQuery(name = "Resubmission.findByType", query = "SELECT r FROM Resubmission r WHERE r.type = :type"),
    @NamedQuery(name = "Resubmission.findByComment", query = "SELECT r FROM Resubmission r WHERE r.comment = :comment"),
    @NamedQuery(name = "Resubmission.findByAttachment", query = "SELECT r FROM Resubmission r WHERE r.attachment = :attachment"),
    @NamedQuery(name = "Resubmission.findById", query = "SELECT r FROM Resubmission r WHERE r.id = :id")})
public class Resubmission implements Serializable {

    private static final long serialVersionUID = 1L;
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

    @Size(max = 200)
    @Column(name = "Comment", length = 200)
    @XmlElement(name = "Comment", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlStringLength.class)
    private String comment;

    @Size(max = 200)
    @Column(name = "Attachment", length = 200)
    @XmlElement(name = "Attachment")
    private byte[] attachment;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Claim claimID;

    @OneToMany(mappedBy = "resubmission", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ResubmissionOutcome> outcome;

    public Resubmission() {
    }

    public Resubmission(Integer id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public byte[] getAttachment() {
        return attachment;
    }

    public void setAttachment(byte[] attachment) {
        this.attachment = attachment;
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
//        if (!(object instanceof Resubmission)) {
//            return false;
//        }
//        Resubmission other = (Resubmission) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Resubmission[ id=" + id + " ]";
//    }

    public List<ResubmissionOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ResubmissionOutcome> outcome) {
        this.outcome = outcome;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new ResubmissionOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ResubmissionOutcome>();
            this.outcome.add(new ResubmissionOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }
}
