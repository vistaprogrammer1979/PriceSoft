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

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "currentNode",
    "action",
    "actionThreshold",
    "workflowNextNode",
    "conditionalWorkflowNextNode",
    "routingRule",
    "outcome"
})

@Entity
@Table(name = "Workflow")

@NamedQueries({
    @NamedQuery(name = "Workflow.findAll", query = "SELECT w FROM Workflow w"),
    @NamedQuery(name = "Workflow.findByCurrentNode", query = "SELECT w FROM Workflow w WHERE w.currentNode = :currentNode"),
    @NamedQuery(name = "Workflow.findByAction", query = "SELECT w FROM Workflow w WHERE w.action = :action"),
    @NamedQuery(name = "Workflow.findByActionThreshold", query = "SELECT w FROM Workflow w WHERE w.actionThreshold = :actionThreshold"),
    @NamedQuery(name = "Workflow.findById", query = "SELECT w FROM Workflow w WHERE w.id = :id")})
public class Workflow implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;
    @Size(max = 200)
    @Column(name = "CurrentNode", length = 200)
    @XmlElement(name = "CurrentNode", required = true)
    private String currentNode;
    @Size(max = 200)
    @Column(name = "Action", length = 200)
    @XmlElement(name = "Action", required = true)
    private String action;
    @Size(max = 200)
    @Column(name = "ActionThreshold", length = 200)
    @XmlElement(name = "ActionThreshold", required = true)
    private String actionThreshold;

    @Size(max = 256)
    @Column(name = "workflowNextNode", length = 256)
    @XmlElement(name = "WorkflowNextNode", required = false)
    private String workflowNextNode;
    
    @Size(max = 256)
    @Column(name = "conditionalWorkflowNextNode", length = 256)
    @XmlElement(name = "conditionalWorkflowNextNode", required = false)
    private String conditionalWorkflowNextNode;

    @Size(max = 256)
    @Column(name = "routingRule", length = 256)
    @XmlElement(name = "RoutingRule", required = false)
    private String routingRule;

    @JoinColumn(name = "headerID", referencedColumnName = "ID")
    @OneToOne
    @XmlTransient
    private Header headerID;

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<WorkflowOutcome> outcome;

    public Workflow() {
    }

    public Workflow(Integer id) {
        this.id = id;
    }

    public String getCurrentNode() {
        return currentNode;
    }

    public void setCurrentNode(String currentNode) {
        this.currentNode = currentNode;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getActionThreshold() {
        return actionThreshold;
    }

    public void setActionThreshold(String actionThreshold) {
        this.actionThreshold = actionThreshold;
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
//        if (!(object instanceof Workflow)) {
//            return false;
//        }
//        Workflow other = (Workflow) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Workflow[ id=" + id + " ]";
//    }

    public List<WorkflowOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<WorkflowOutcome> outcome) {
        this.outcome = outcome;
    }

    public String getWorkflowNextNode() {
        return workflowNextNode;
    }

    public void setWorkflowNextNode(String workflowNextNode) {
        this.workflowNextNode = workflowNextNode;
    }

    public String getConditionalWorkflowNextNode() {
        return conditionalWorkflowNextNode;
    }

    public void setConditionalWorkflowNextNode(String conditionalWorkflowNextNode) {
        this.conditionalWorkflowNextNode = conditionalWorkflowNextNode;
    }
    
    

    public String getRoutingRule() {
        return routingRule;
    }

    public void setRoutingRule(String routingRule) {
        this.routingRule = routingRule;
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new WorkflowOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<WorkflowOutcome>();
            this.outcome.add(new WorkflowOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

}
