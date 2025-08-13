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
import javax.xml.bind.annotation.XmlTransient;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author smutlak
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "activityID",
    "idCaller",
    "SPCFactor",
    "discountPercentage",
    "discount",
    "specialDiscountAmount",
    "specialDiscountPercentage",
    "drugType",
    "start",
    "type",
    "code",
    "quantity",
    "net",
    "providerNet",
    "clinician",
    "orderingClinician",
    "priorAuthorizationID",
    "drg_weight",
    "timeUnits",
    "anaesthesiaBaseUnits",
    "list",
    "gross",
    "patientShare",
    "paymentAmount",
    "denialCode",
    "copayment",
    "deductible",
    "EX_PBP",
    "manualPrices",
    "procedureType",
    "providerType",
    "providerCode",
    "providerPatientShare",
    "activityGroup",
    "observation",
    "custom_Price_Types",
    "rejected_Amount",
    "packageGroupID",
    "orderStatus",
    "outcome"
})

@Entity
@Table(name = "Activity")

@NamedQueries({
    @NamedQuery(name = "Activity.findAll", query = "SELECT a FROM Activity a"),
    @NamedQuery(name = "Activity.findById", query = "SELECT a FROM Activity a WHERE a.id = :id"),
    @NamedQuery(name = "Activity.findByStart", query = "SELECT a FROM Activity a WHERE a.start = :start"),
    @NamedQuery(name = "Activity.findByType", query = "SELECT a FROM Activity a WHERE a.type = :type"),
    @NamedQuery(name = "Activity.findByCode", query = "SELECT a FROM Activity a WHERE a.code = :code"),
    @NamedQuery(name = "Activity.findByQuantity", query = "SELECT a FROM Activity a WHERE a.quantity = :quantity"),
    @NamedQuery(name = "Activity.findByNet", query = "SELECT a FROM Activity a WHERE a.net = :net"),
    @NamedQuery(name = "Activity.findByProviderNet", query = "SELECT a FROM Activity a WHERE a.providerNet = :providerNet"),
    @NamedQuery(name = "Activity.findByClinician", query = "SELECT a FROM Activity a WHERE a.clinician = :clinician"),
    @NamedQuery(name = "Activity.findByOrderingClinician", query = "SELECT a FROM Activity a WHERE a.orderingClinician = :orderingClinician"),
    @NamedQuery(name = "Activity.findByPriorAuthorizationID", query = "SELECT a FROM Activity a WHERE a.priorAuthorizationID = :priorAuthorizationID"),
    @NamedQuery(name = "Activity.findByList", query = "SELECT a FROM Activity a WHERE a.list = :list"),
    @NamedQuery(name = "Activity.findByGross", query = "SELECT a FROM Activity a WHERE a.gross = :gross"),
    @NamedQuery(name = "Activity.findByPatientShare", query = "SELECT a FROM Activity a WHERE a.patientShare = :patientShare"),
    @NamedQuery(name = "Activity.findByPaymentAmount", query = "SELECT a FROM Activity a WHERE a.paymentAmount = :paymentAmount"),
    @NamedQuery(name = "Activity.findByDenialCode", query = "SELECT a FROM Activity a WHERE a.denialCode = :denialCode"),
    @NamedQuery(name = "Activity.findByCopayment", query = "SELECT a FROM Activity a WHERE a.copayment = :copayment"),
    @NamedQuery(name = "Activity.findByDeductible", query = "SELECT a FROM Activity a WHERE a.deductible = :deductible"),
    @NamedQuery(name = "Activity.findByManualPrices", query = "SELECT a FROM Activity a WHERE a.manualPrices = :manualPrices"),
    @NamedQuery(name = "Activity.findById", query = "SELECT a FROM Activity a WHERE a.id = :id")})
public class Activity implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @NotNull
    @Column(name = "ID", nullable = false)
    @XmlTransient
    private Integer id;

    @Column(name = "activityID", nullable = false)
    @XmlElement(name = "ID", required = true)
    protected String activityID;

    @Column(name = "idCaller")
    @XmlElement(name = "idCaller", required = false)
    private Integer idCaller;

    @Column(name = "SPCFactor")
    @XmlElement(name = "SPCFactor", required = false)
    private Double SPCFactor;

    @Column(name = "DiscountPercentage")
    @XmlElement(name = "DiscountPercentage", required = false)
    private Double discountPercentage;

    @Column(name = "Discount")
    @XmlElement(name = "Discount", required = false)
    private Double discount;
    @Column(name = "SpecialDiscountAmount")
    @XmlElement(name = "SpecialDiscountAmount", required = false)
    private Double specialDiscountAmount;
    @Column(name = "SpecialDiscountPercentage")
    @XmlElement(name = "SpecialDiscountPercentage", required = false)
    private Double specialDiscountPercentage;
    @Column(name = "DrugType")
    @XmlElement(name = "DrugType", required = false)
    private Integer drugType;

    @Size(max = 200)
    @Column(name = "Start", length = 200)
    @Temporal(TemporalType.TIMESTAMP)
    @XmlElement(name = "Start", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)
    private Date start;

    @Column(name = "Type")
    @XmlElement(name = "Type", required = true)
    @XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlCodeTypeAdapter.class)
    private CodeType type;

    @Size(max = 200)
    @Column(name = "Code", length = 200)
    @XmlElement(name = "Code", required = true)
    private String code;
    @Size(max = 200)
    @Column(name = "Custom_Price_Types", length = 200)
    @XmlElement(name = "Custom_Price_Types", required = true)
    private String custom_Price_Types;
     @Column(name = "Rejected_Amount", precision = 53)
    @XmlElement(name = "Rejected_Amount")
    private Double  rejected_Amount;

    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "Quantity", precision = 53)
    @XmlElement(name = "Quantity")
    private Double quantity;

    @Column(name = "Net", precision = 53)
    @XmlElement(name = "Net")
    private Double net;

    @Column(name = "ProviderNet", precision = 53)
    @XmlElement(name = "ProviderNet", required = false)
    private Double providerNet;

    @Size(max = 200)
    @Column(name = "Clinician", length = 200)
    @XmlElement(name = "Clinician", required = true)
    private String clinician;

    @Size(max = 200)
    @Column(name = "OrderingClinician", length = 200)
    @XmlElement(name = "OrderingClinician", required = false)
    private String orderingClinician;

    @Size(max = 200)
    @Column(name = "PriorAuthorizationID", length = 200)
    @XmlElement(name = "PriorAuthorizationID")
    private String priorAuthorizationID;

    @Column(name = "Drg_weight")
    @XmlElement(name = "Drg_weight", required = false)
    private Double drg_weight;

    @Column(name = "TimeUnits")
    @XmlElement(name = "TimeUnits", required = false)
    private Integer timeUnits;

    @Column(name = "AnaesthesiaBaseUnits", precision = 53)
    @XmlElement(name = "AnaesthesiaBaseUnits")
    private Double anaesthesiaBaseUnits;

    @Column(name = "List", precision = 53)
    @XmlElement(name = "List")
    private Double list;

    @Column(name = "Gross", precision = 53)
    @XmlElement(name = "Gross")
    private Double gross;

    @Column(name = "PatientShare", precision = 53)
    @XmlElement(name = "PatientShare")
    private Double patientShare;

    @Column(name = "PaymentAmount", precision = 53)
    @XmlElement(name = "PaymentAmount")
    private Double paymentAmount;

    @Size(max = 200)
    @Column(name = "DenialCode", length = 200)
    @XmlElement(name = "DenialCode")
    private String denialCode;

    @Column(name = "Copayment", precision = 53)
    @XmlElement(name = "Copayment")
    private Double copayment;

    @Column(name = "deductible", precision = 53)
    @XmlElement(name = "deductible")
    private Double deductible;

    @Column(name = "EX_PBP", precision = 53)
    @XmlElement(name = "EX_PBP")
    private Double EX_PBP;

    @Column(name = "ManualPrices")
    @XmlElement(name = "ManualPrices")
    private Boolean manualPrices; 
    @Column(name = "packageGroupID")
    @XmlElement(name = "packageGroupID")
    private int  packageGroupID;

    @Column(name = "ProcedureType")
    @XmlElement(name = "ProcedureType")
    private Integer procedureType;

    @Column(name = "providerType")
    @XmlElement(name = "providerType", required = false)
    private Integer providerType;

    @Column(name = "providerCode", length = 200)
    @XmlElement(name = "providerCode", required = false)
    private String providerCode;
    @Column(name = "OrderStatus", length = 200)
    @XmlElement(name = "OrderStatus", required = false)
    private String orderStatus;
    

    @Column(name = "providerPatientShare", precision = 53)
    @XmlElement(name = "providerPatientShare", required = false)
    private Double providerPatientShare;

    @OneToMany(mappedBy = "activityID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "ActivityGroup")
    private List<ActivityGroup> activityGroup;

    @OneToMany(mappedBy = "activityID", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Observation")
    private List<Observation> observation;

    @JoinColumn(name = "claimID", referencedColumnName = "ID")
    @ManyToOne
    @XmlTransient
    private Claim claimID;

    @Column(name = "ListPricePredifined", nullable = true)
    @XmlTransient
    private Integer listPricePredifined;

    @OneToMany(mappedBy = "activity", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @XmlElement(name = "Outcome", required = false)
    private List<ActivityOutcome> outcome;

    public String getCustom_Price_Types() {
        return custom_Price_Types;
    }

    public void setCustom_Price_Types(String custom_Price_Types) {
        this.custom_Price_Types = custom_Price_Types;
    }

    public Activity() {
    }

    public String getActivityID() {
        return activityID;
    }

    public void setActivityID(String activityID) {
        this.activityID = activityID;
    }

    public List<ActivityOutcome> getOutcome() {
        return outcome;
    }

    public void setOutcome(List<ActivityOutcome> outcome) {
        this.outcome = outcome;
    }

    public Integer getId() {
        return id;
    }

    public Double getSpecialDiscountAmount() {
        return specialDiscountAmount;
    }

    public void setSpecialDiscountAmount(Double specialDiscountAmount) {
        this.specialDiscountAmount = specialDiscountAmount;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getIdCaller() {
        return idCaller;
    }

    public void setIdCaller(Integer idCaller) {
        this.idCaller = idCaller;
    }

    public Double getSPCFactor() {
        return SPCFactor;
    }

    public void setSPCFactor(Double SPCFactor) {
        this.SPCFactor = SPCFactor;
    }

    public Double getDiscount() {
        return discount;
    }

    public void setDiscount(Double discount) {
        this.discount = discount;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getQuantity() {
        return quantity;
    }

    public void setQuantity(Double quantity) {
        this.quantity = quantity;
    }

    public Double getNet() {
        return net;
    }

    public void setNet(Double net) {
        this.net = net;
    }

    public Double getProviderNet() {
        return providerNet;
    }

    public void setProviderNet(Double providerNet) {
        this.providerNet = providerNet;
    }

    public String getClinician() {
        return clinician;
    }

    public void setClinician(String clinician) {
        this.clinician = clinician;
    }

    public String getOrderingClinician() {
        return orderingClinician;
    }

    public void setOrderingClinician(String orderingClinician) {
        this.orderingClinician = orderingClinician;
    }

    public String getPriorAuthorizationID() {
        return priorAuthorizationID;
    }

    public void setPriorAuthorizationID(String priorAuthorizationID) {
        this.priorAuthorizationID = priorAuthorizationID;
    }

    public Double getList() {
        return list;
    }

    public void setList(Double list) {
        this.list = list;
    }

    public Double getGross() {
        return gross;
    }

    public Double getGrossMinusDeductible() {
        if (deductible != null && !deductible.isNaN() && deductible > 0d) {
            return gross - deductible;
        }
        return gross;
    }

    public void setGross(Double gross) {
        this.gross = gross;
    }

    public Double getPatientShare() {
        return patientShare;
    }

    public void setPatientShare(Double patientShare) {
        this.patientShare = patientShare;
    }

    public Double getPaymentAmount() {
        return paymentAmount;
    }

    public void setPaymentAmount(Double paymentAmount) {
        this.paymentAmount = paymentAmount;
    }

    public Double getRejected_Amount() {
        return rejected_Amount;
    }

    public void setRejected_Amount(Double rejected_Amount) {
        this.rejected_Amount = rejected_Amount;
    }

    public String getDenialCode() {
        return denialCode;
    }

    public void setDenialCode(String denialCode) {
        this.denialCode = denialCode;
    }

    public Double getCopayment() {
        return copayment;
    }

    public void setCopayment(Double copayment) {
        this.copayment = copayment;
    }

    public Double getDeductible() {
        return deductible;
    }

    public int getPackageGroupID() {
        return packageGroupID;
    }

    public void setPackageGroupID(int packageGroupID) {
        this.packageGroupID = packageGroupID;
    }

    

    public void setDeductible(Double deductible) {
        this.deductible = deductible;
    }

    public Boolean getManualPrices() {
        return manualPrices;
    }

    public void setManualPrices(Boolean manualPrices) {
        this.manualPrices = manualPrices;
    }

    public Integer getProcedureType() {
        return procedureType;
    }

    public void setProcedureType(Integer procedureType) {
        this.procedureType = procedureType;
    }

    public List<Observation> getObservation() {
        return observation;
    }

    public void setObservation(List<Observation> observation) {
        this.observation = observation;
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
//        if (!(object instanceof Activity)) {
//            return false;
//        }
//        Activity other = (Activity) object;
//        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "com.accumed.model.request.Activity[ id=" + id + " ]";
//    }

    public void addSingleRuleOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        ActivityOutcome aout = new ActivityOutcome(severity, ruleName, ShortMsg, longMsg);
        if (!isExists(aout)) {
            this.addOutcome(severity, ruleName, ShortMsg, longMsg);
        }
    }

    public void addOutcome(String severity, String ruleName, String ShortMsg, String longMsg) {
        longMsg = !longMsg.isEmpty() ? " ," + this.toString() : this.toString();
        if (this.outcome != null) {
            this.outcome.add(new ActivityOutcome(severity, ruleName, ShortMsg, longMsg));
        } else {
            this.outcome = new ArrayList<ActivityOutcome>();
            this.outcome.add(new ActivityOutcome(severity, ruleName, ShortMsg, longMsg));
        }
    }

    private boolean isExists(ActivityOutcome aout) {
        if (outcome != null) {
            for (ActivityOutcome tmp : outcome) {
                if (tmp.getRuleName().equals(aout.getRuleName())) {
                    /*&& tmp.getShortMsg().equals(aout.getShortMsg())
                     && (tmp.getLongMsg() == null ? aout.getLongMsg() == null : tmp.getLongMsg().equals(aout.getLongMsg()))) {*/
                    return true;
                }
            }
        }
        return false;
    }

    public boolean addActivityGroup(Integer id, Integer type, String name) {
        if (activityGroup != null) {
            for (ActivityGroup ag : activityGroup) {
                if (ag.getId().equals(id)) {
                    return false;
                }
            }
        }

        if (this.activityGroup != null) {
            this.activityGroup.add(new ActivityGroup(id, type, name, this));
        } else {
            this.activityGroup = new ArrayList<ActivityGroup>();
            this.activityGroup.add(new ActivityGroup(id, type, name, this));
        }
        return true;
    }

    public Integer getProviderType() {
        return providerType;
    }

    public void setProviderType(Integer providerType) {
        this.providerType = providerType;
    }

    public String getProviderCode() {
        return providerCode;
    }

    public void setProviderCode(String providerCode) {
        this.providerCode = providerCode;
    }

    public Double getProviderPatientShare() {
        return providerPatientShare;
    }

    public void setProviderPatientShare(Double providerPatientShare) {
        this.providerPatientShare = providerPatientShare;
    }

    public List<ActivityGroup> getActivityGroup() {
        return activityGroup;
    }

    public void setActivityGroup(List<ActivityGroup> activityGroup) {
        this.activityGroup = activityGroup;
    }

    public Integer getDrugType() {
        return drugType;
    }

    public void setDrugType(Integer drugType) {
        this.drugType = drugType;
    }

    public Double getDiscountPercentage() {
        return discountPercentage;
    }

    public void setDiscountPercentage(Double discountPercentage) {
        this.discountPercentage = discountPercentage;
    }

    public boolean isMemberOf(CodeType groupType, String groupName) {
        if (activityGroup == null || activityGroup.isEmpty()) {
            return false;
        }

        for (ActivityGroup group : activityGroup) {
            if (group.getType() == groupType.getValue() && group.getName().equals(groupName)) {
                return true;
            }
        }
        return false;
    }

    public Integer getTimeUnits() {
        return timeUnits;
    }

    public void setTimeUnits(Integer timeUnits) {
        this.timeUnits = timeUnits;
    }

    public Double getAnaesthesiaBaseUnits() {
        return anaesthesiaBaseUnits;
    }

    public void setAnaesthesiaBaseUnits(Double anaesthesiaBaseUnits) {
        this.anaesthesiaBaseUnits = anaesthesiaBaseUnits;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    @Override
    public String toString() {
        return "For Activity{" + "id=" + idCaller + ", code=" + code + '}';
    }

    public Double getEX_PBP() {
        return EX_PBP;
    }

    public void setEX_PBP(Double EX_PBP) {
        this.EX_PBP = EX_PBP;
    }

    public Integer getListPricePredifined() {
        return listPricePredifined;
    }

    public void setListPricePredifined(Integer listPricePredifined) {
        this.listPricePredifined = listPricePredifined;
    }

    public Double getDrg_weight() {
        return drg_weight;
    }

    public void setDrg_weight(Double drg_weight) {
        this.drg_weight = drg_weight;
    }

    public Double getSpecialDiscountPercentage() {
        return specialDiscountPercentage;
    }

    public void setSpecialDiscountPercentage(Double specialDiscountPercentage) {
        this.specialDiscountPercentage = specialDiscountPercentage;
    }

    

    
    

}
