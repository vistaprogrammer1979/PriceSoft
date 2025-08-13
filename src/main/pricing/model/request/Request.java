/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

//import com.accumed.model.Severity;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
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
    "header",
    "claim",
    "outcome"
})
@XmlRootElement(name = "Request")
@Entity
@Table(name = "Request")
@NamedQueries({
    @NamedQuery(name = "Request.findAll", query = "SELECT v FROM Request v"),
    @NamedQuery(name = "Request.findById", query = "SELECT v FROM Request v WHERE v.id = :id"),
    @NamedQuery(name = "Request.findByClaimsCount", query = "SELECT v FROM Request v WHERE v.claimsCount = :claimsCount"),
    @NamedQuery(name = "Request.findByStartDate", query = "SELECT v FROM Request v WHERE v.startDate = :startDate"),
    @NamedQuery(name = "Request.findByEndDate", query = "SELECT v FROM Request v WHERE v.endDate = :endDate"),
    @NamedQuery(name = "Request.findByInvalidClaimsCount", query = "SELECT v FROM Request v WHERE v.invalidClaimsCount = :invalidClaimsCount"),
    @NamedQuery(name = "Request.findByUserId", query = "SELECT v FROM Request v WHERE v.userId = :userId"),
    @NamedQuery(name = "Request.findByRequest", query = "SELECT v FROM Request v WHERE v.request = :request")})
public class Request implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;
    @Column(name = "claimsCount")
    @XmlTransient
    private Integer claimsCount;
    @Column(name = "startDate")
    @Temporal(TemporalType.TIMESTAMP)
    @XmlTransient
    private Date startDate;
    @Column(name = "endDate")
    @Temporal(TemporalType.TIMESTAMP)
    @XmlTransient
    private Date endDate;
    @Column(name = "invalidClaimsCount")
    @XmlTransient
    private Integer invalidClaimsCount;
    @Column(name = "user_id")
    @XmlTransient
    private Integer userId;

    @Size(max = 1073741823)
    @Column(name = "request", columnDefinition = "xml")
    @XmlTransient
    private String request;

    @OneToOne(mappedBy = "requestID", cascade = CascadeType.PERSIST)
    @XmlElement(name = "Header", required = true)
    private Header header;

    @OneToMany(mappedBy = "request", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER )
    @XmlElement(name = "Claim", required = true)
    private List<Claim> claim;

    @OneToMany(mappedBy = "request", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<RequestOutcome> outcome;

    public Request() {
    }

    public Request(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getClaimsCount() {
        return claimsCount;
    }

    public void setClaimsCount(Integer claimsCount) {
        this.claimsCount = claimsCount;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Integer getInvalidClaimsCount() {
        return invalidClaimsCount;
    }

    public void setInvalidClaimsCount(Integer invalidClaimsCount) {
        this.invalidClaimsCount = invalidClaimsCount;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public List<Claim> getClaim() {
        return claim;
    }

    public void setClaim(List<Claim> claim) {
        this.claim = claim;
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
//        if (!(object instanceof Request)) {
//            return false;
//        }
//        Request other = (Request) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Request[ id=" + id + " ]";
//    }

    public List<RequestOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<RequestOutcome> outcome) {
        this.outcome = outcome;
    }
    
    public boolean Is20() {

        int total = 0;
        if (getOutcome() != null) {
            total+=getOutcome().size();
            if(total>=20){ return true;}
        }

        if (getHeader() != null) {
            if (getHeader().getOutcome() != null) {
                total+=getHeader().getOutcome().size();
                if(total>=20){ return true;}
            }
            if (getHeader().getWorkflow() != null) {
                if (getHeader().getWorkflow().getOutcome() != null) {
                    total+=getHeader().getWorkflow().getOutcome().size();
                    if(total>=20){ return true;}
                }
            }

            if (getHeader().getExtendedValidationType() != null) {
                for (com.accumed.pricing.model.request.ExtendedValidationType extendedValidationType : getHeader().getExtendedValidationType()) {
                    if (extendedValidationType.getOutcome() != null) {
                        total+=extendedValidationType.getOutcome().size();
                        if(total>=20){ return true;}
                    }
                }
            }
        }

        //Claim
        if (getClaim() != null) {
            for (com.accumed.pricing.model.request.Claim claim : getClaim()) {
                if (claim.getOutcome() != null) {
                    total+=claim.getOutcome().size();
                    if(total>=20){ return true;}
                }

                if (claim.getEncounter() != null) {
                    for (com.accumed.pricing.model.request.Encounter encounter : claim.getEncounter()) {
                        if (encounter.getOutcome() != null) {
                            total+=encounter.getOutcome().size();
                            if(total>=20){ return true;}
                        }

                        if (encounter.getAuthorisation() != null) {
                            if (encounter.getAuthorisation().getOutcome() != null) {
                                total+=encounter.getAuthorisation().getOutcome().size();
                                if(total>=20){ return true;}
                            }
                        }
                    }
                }

                if (claim.getDiagnosis() != null) {
                    for (com.accumed.pricing.model.request.Diagnosis diagnosis : claim.getDiagnosis()) {
                        if (diagnosis.getOutcome() != null) {
                            total+=diagnosis.getOutcome().size();
                            if(total>=20){ return true;}
                        }
                    }
                }

                if (claim.getActivity() != null) {
                    for (com.accumed.pricing.model.request.Activity activity : claim.getActivity()) {
                        if (activity.getOutcome() != null) {
                            total+=activity.getOutcome().size();
                            if(total>=20){ return true;}
                        }

                        if (activity.getObservation() != null) {
                            for (com.accumed.pricing.model.request.Observation observation : activity.getObservation()) {
                                if (observation.getOutcome() != null) {
                                    total+=observation.getOutcome().size();
                                    if(total>=20){ return true;}
                                }
                            }
                        }
                    }
                }

                if (claim.getResubmission() != null) {
                    if (claim.getResubmission().getOutcome() != null) {
                        total+=claim.getResubmission().getOutcome().size();
                        if(total>=20){ return true;}
                    }
                }
                if (claim.getContract() != null) {
                    if (claim.getContract().getOutcome() != null) {
                        total+=claim.getContract().getOutcome().size();
                        if(total>=20){ return true;}
                    }
                }
                if (claim.getPatient() != null) {
                    if (claim.getPatient().getOutcome() != null) {
                        total+=claim.getPatient().getOutcome().size();
                        if(total>=20){ return true;}
                    }

                    if (claim.getPatient().getPatientInsurance() != null) {
                        if (claim.getPatient().getPatientInsurance().getOutcome() != null) {
                            total+=claim.getPatient().getPatientInsurance().getOutcome().size();
                            if(total>=20){ return true;}
                        }

                    }
                }
            }
        }
        return total>=20;
    }

    public void fixRequest()
    {
        if (getClaim() != null) {
            for (com.accumed.pricing.model.request.Claim claim : getClaim()) {
                if(claim.getActivity() == null){
                    claim.setActivity(new ArrayList<Activity>());
                }
            }
        }
    }
    public boolean removeAllOutcomes() {

        if (getOutcome() != null) {
            setOutcome(null);
        }

        if (getHeader() != null) {
            if (getHeader().getOutcome() != null) {
                getHeader().setOutcome(null);
            }
            if (getHeader().getWorkflow() != null) {
                if (getHeader().getWorkflow().getOutcome() != null) {
                    getHeader().getWorkflow().setOutcome(null);
                }
            }

            if (getHeader().getExtendedValidationType() != null) {
                for (com.accumed.pricing.model.request.ExtendedValidationType extendedValidationType : getHeader().getExtendedValidationType()) {
                    if (extendedValidationType.getOutcome() != null) {
                        extendedValidationType.setOutcome(null);
                    }
                }
            }
        }

        //Claim
        if (getClaim() != null) {
            for (com.accumed.pricing.model.request.Claim claim : getClaim()) {
                if (claim.getOutcome() != null) {
                    claim.setOutcome(null);
                }

                if (claim.getEncounter() != null) {
                    for (com.accumed.pricing.model.request.Encounter encounter : claim.getEncounter()) {
                        if (encounter.getOutcome() != null) {
                            encounter.setOutcome(null);
                        }

                        if (encounter.getAuthorisation() != null) {
                            if (encounter.getAuthorisation().getOutcome() != null) {
                                encounter.getAuthorisation().setOutcome(null);
                            }
                        }
                    }
                }

                if (claim.getDiagnosis() != null) {
                    for (com.accumed.pricing.model.request.Diagnosis diagnosis : claim.getDiagnosis()) {
                        if (diagnosis.getOutcome() != null) {
                            diagnosis.setOutcome(null);
                        }
                    }
                }

                if (claim.getActivity() != null) {
                    for (com.accumed.pricing.model.request.Activity activity : claim.getActivity()) {
                        if (activity.getOutcome() != null) {
                            activity.setOutcome(null);
                        }

                        if (activity.getObservation() != null) {
                            for (com.accumed.pricing.model.request.Observation observation : activity.getObservation()) {
                                if (observation.getOutcome() != null) {
                                    observation.setOutcome(null);
                                }
                            }
                        }
                    }
                }

                if (claim.getResubmission() != null) {
                    if (claim.getResubmission().getOutcome() != null) {
                        claim.getResubmission().setOutcome(null);
                    }
                }
                if (claim.getContract() != null) {
                    if (claim.getContract().getOutcome() != null) {
                        claim.getContract().setOutcome(null);
                    }
                }
                if (claim.getPatient() != null) {
                    if (claim.getPatient().getOutcome() != null) {
                        claim.getPatient().setOutcome(null);
                    }

                    if (claim.getPatient().getPatientInsurance() != null) {
                        if (claim.getPatient().getPatientInsurance().getOutcome() != null) {
                            claim.getPatient().getPatientInsurance().setOutcome(null);
                        }

                    }
                }
            }
        }
        return true;
    }
    
//    public com.accumed.model.batch.outcome.Response getAllOutcomes(){
//        com.accumed.model.batch.outcome.Response response  = new com.accumed.model.batch.outcome.Response();
//        response.setOutcome(new ArrayList());
//        
//        if (getOutcome() != null) {
//            response.addRequestOutcome(getOutcome());
//        }
//
//        if (getHeader() != null) {
//            if (getHeader().getOutcome() != null) {
//                response.addHeaderOutcome(getHeader().getOutcome());
//            }
//            if (getHeader().getWorkflow() != null) {
//                if (getHeader().getWorkflow().getOutcome() != null) {
//                    response.addWorkflowOutcome(getHeader().getWorkflow().getOutcome());
//                }
//            }
//
//            if (getHeader().getExtendedValidationType() != null) {
//                for (com.accumed.model.request.ExtendedValidationType extendedValidationType : getHeader().getExtendedValidationType()) {
//                    if (extendedValidationType.getOutcome() != null) {
//                        response.addExtendedValidationTypeOutcome(extendedValidationType.getOutcome());
//                    }
//                }
//            }
//        }
//
//        //Claim
//        if (getClaim() != null) {
//            for (com.accumed.model.request.Claim claim : getClaim()) {
//                
//                com.accumed.model.batch.outcome.ResponseClaim responseClaim = new com.accumed.model.batch.outcome.ResponseClaim();
//                responseClaim.setID(claim.getRootID());
//                responseClaim.setIdCaller(claim.getIdCaller());
//                
//                if (claim.getOutcome() != null) {
//                    responseClaim.addClaimOutcome(claim.getOutcome());
//                }
//
//                if (claim.getEncounter() != null) {
//                    for (com.accumed.model.request.Encounter encounter : claim.getEncounter()) {
//                        if (encounter.getOutcome() != null) {
//                            responseClaim.addEncounterOutcome(encounter.getOutcome());
//                            
//                        }
//
//                        if (encounter.getAuthorisation() != null) {
//                            if (encounter.getAuthorisation().getOutcome() != null) {
//                                responseClaim.addAuthorisationOutcome(encounter.getAuthorisation().getOutcome());
//                            }
//                        }
//                    }
//                }
//
//                if (claim.getDiagnosis() != null) {
//                    for (com.accumed.model.request.Diagnosis diagnosis : claim.getDiagnosis()) {
//                        if (diagnosis.getOutcome() != null) {
//                            responseClaim.addDiagnosisOutcome(diagnosis.getOutcome());
//                        }
//                    }
//                }
//
//                if (claim.getActivity() != null) {
//                    for (com.accumed.model.request.Activity activity : claim.getActivity()) {
//                        if (activity.getOutcome() != null) {
//                            responseClaim.addActivityOutcome(activity.getOutcome());
//                        }
//
//                        if (activity.getObservation() != null) {
//                            for (com.accumed.model.request.Observation observation : activity.getObservation()) {
//                                if (observation.getOutcome() != null) {
//                                    responseClaim.addObservationOutcome(observation.getOutcome());
//                                }
//                            }
//                        }
//                    }
//                }
//
//                if (claim.getResubmission() != null) {
//                    if (claim.getResubmission().getOutcome() != null) {
//                        responseClaim.addResubmissionOutcome(claim.getResubmission().getOutcome());
//                    }
//                }
//                if (claim.getContract() != null) {
//                    if (claim.getContract().getOutcome() != null) {
//                        responseClaim.addContractOutcome(claim.getContract().getOutcome());
//                    }
//                }
//                if (claim.getPatient() != null) {
//                    if (claim.getPatient().getOutcome() != null) {
//                        responseClaim.addPatientOutcome(claim.getPatient().getOutcome());
//                    }
//
//                    if (claim.getPatient().getPatientInsurance() != null) {
//                        if (claim.getPatient().getPatientInsurance().getOutcome() != null) {
//                            responseClaim.addPatientInsuranceOutcome(claim.getPatient().getPatientInsurance().getOutcome());
//                        }
//                    }
//                }
//                response.getClaims().add(responseClaim);
//            }
//        }
//        return response;
//    }

//    public boolean ContainsOrAbove(String level) {
//
//        String[] arrValues;
//        if (level.compareToIgnoreCase("BLOCKER") == 0) {
//            arrValues = new String[]{"BLOCKER"};
//        } else if (level.compareToIgnoreCase("CRITICAL") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL"};
//        } else if (level.compareToIgnoreCase("SEVERE") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE"};
//        } else if (level.compareToIgnoreCase("WARNING") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING"};
//        } else if (level.compareToIgnoreCase("INFO") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING", "INFO"};
//        } else if (level.compareToIgnoreCase("CONFIG") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING", "INFO", "CONFIG"};
//        } else if (level.compareToIgnoreCase("FINE") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE"};
//        } else if (level.compareToIgnoreCase("FINER") == 0) {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER"};
//        } else /*if (level.compareToIgnoreCase("FINEST")==0)*/ {
//            arrValues = new String[]{"BLOCKER", "CRITICAL", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST"};
//        }
//
//        Set<String> hashSetValues = new HashSet<String>(Arrays.asList(arrValues));
//
//        com.accumed.model.batch.outcome.Response response = getAllOutcomes();
//        java.util.Iterator<com.accumed.model.batch.outcome.ResponseClaim> iter = response.getClaims().iterator();
//        while (iter.hasNext()) {
//            com.accumed.model.batch.outcome.ResponseClaim cr = iter.next();
//            java.util.Iterator<com.accumed.model.batch.outcome.Outcome> res_iter = cr.getOutcome().iterator();
//            while (res_iter.hasNext()) {
//                com.accumed.model.batch.outcome.Outcome res = res_iter.next();
//                //if (hashSetValues.contains(res.getSeverity())) {
//                if (containsIgnoreCase(hashSetValues, res.getSeverity())) {
//                    return true;
//                }
//            }
//
//        }
//        return false;
//    }

    public boolean containsIgnoreCase(Set<String> l, String s) {
        Iterator<String> it = l.iterator();
        while (it.hasNext()) {
            if (it.next().equalsIgnoreCase(s)) {
                return true;
            }
        }
        return false;
    }

//    public void addOutcome(Exception e) {
//        if (this.outcome != null) {
//            this.outcome.add(new RequestOutcome(e));
//        } else {
//            this.outcome = new ArrayList<RequestOutcome>();
//            this.outcome.add(new RequestOutcome(e));
//        }
//    }

//    public void addOutcome(String ShortMsg) {
//        addOutcome(Severity.BLOCKER, "General", ShortMsg, "");
//    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        if (this.outcome != null) {
            this.outcome.add(new RequestOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<RequestOutcome>();
            this.outcome.add(new RequestOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    public Boolean onlyTop20() {
        if (getHeader().getTop20() == null || getHeader().getTop20() == 0) {
            return false;
        }
        if (getClaim() == null) {
            return false;
        }
        if (getClaim().size() > 1) {
            return false;
        }
        return true;
    }
}
