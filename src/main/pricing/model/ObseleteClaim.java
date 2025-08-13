/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author smutlak
 */
/**
 * <p>Java class for anonymous complex type.
 *
 * <p>The following schema fragment specifies the expected content contained
 * within this class.
 *
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="ID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ClaimLineID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="IDPayer" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="MemberID" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="AccumedPatientID" type="{http://www.w3.org/2001/XMLSchema}long" minOccurs="0"/>
 *         &lt;element name="PayerID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ProviderID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="EmiratesIDNumber" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Gross" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="PatientShare" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="Net" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="DenialCode" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="PaymentReference" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="DateSettlement" minOccurs="0">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *               &lt;minLength value="0"/>
 *               &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="ObseleteEncounter" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="AccumedID" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *                   &lt;element name="FacilityID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="Type">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *                         &lt;enumeration value="1"/>
 *                         &lt;enumeration value="2"/>
 *                         &lt;enumeration value="3"/>
 *                         &lt;enumeration value="4"/>
 *                         &lt;enumeration value="5"/>
 *                         &lt;enumeration value="6"/>
 *                         &lt;enumeration value="7"/>
 *                         &lt;enumeration value="8"/>
 *                         &lt;enumeration value="9"/>
 *                         &lt;enumeration value="12"/>
 *                         &lt;enumeration value="13"/>
 *                         &lt;enumeration value="15"/>
 *                         &lt;enumeration value="41"/>
 *                         &lt;enumeration value="42"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="PatientID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="Start">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;minLength value="0"/>
 *                         &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="End" minOccurs="0">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;minLength value="0"/>
 *                         &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="StartType" minOccurs="0">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *                         &lt;enumeration value="1"/>
 *                         &lt;enumeration value="2"/>
 *                         &lt;enumeration value="3"/>
 *                         &lt;enumeration value="4"/>
 *                         &lt;enumeration value="5"/>
 *                         &lt;enumeration value="6"/>
 *                         &lt;enumeration value="7"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="EndType" minOccurs="0">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *                         &lt;enumeration value="1"/>
 *                         &lt;enumeration value="2"/>
 *                         &lt;enumeration value="3"/>
 *                         &lt;enumeration value="4"/>
 *                         &lt;enumeration value="5"/>
 *                         &lt;enumeration value="6"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="TransferSource" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="TransferDestination" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="ObseleteDiagnosis" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="Type">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;enumeration value="Principal"/>
 *                         &lt;enumeration value="Secondary"/>
 *                         &lt;enumeration value="Admitting"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="Code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="ObseleteActivity" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="AccumedID" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *                   &lt;element name="ID" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="Start">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;minLength value="0"/>
 *                         &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="Type">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *                         &lt;enumeration value="1"/>
 *                         &lt;enumeration value="2"/>
 *                         &lt;enumeration value="3"/>
 *                         &lt;enumeration value="4"/>
 *                         &lt;enumeration value="5"/>
 *                         &lt;enumeration value="6"/>
 *                         &lt;enumeration value="8"/>
 *                         &lt;enumeration value="9"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="Code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="Quantity" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *                   &lt;element name="Net" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *                   &lt;element name="Clinician" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="PriorAuthorizationID" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="List" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *                   &lt;element name="Gross" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *                   &lt;element name="PatientShare" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *                   &lt;element name="PaymentAmount" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *                   &lt;element name="DenialCode" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="Observation" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence>
 *                             &lt;element name="Type">
 *                               &lt;simpleType>
 *                                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                                   &lt;enumeration value="CPT"/>
 *                                   &lt;enumeration value="HL7v3 Native"/>
 *                                   &lt;enumeration value="LOINC"/>
 *                                   &lt;enumeration value="SNOMED CT"/>
 *                                   &lt;enumeration value="Text"/>
 *                                   &lt;enumeration value="File"/>
 *                                   &lt;enumeration value="Universal Dental"/>
 *                                 &lt;/restriction>
 *                               &lt;/simpleType>
 *                             &lt;/element>
 *                             &lt;element name="Code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                             &lt;element name="Value" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                             &lt;element name="ValueType" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                           &lt;/sequence>
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="Resubmission" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="Type">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;enumeration value="correction"/>
 *                         &lt;enumeration value="internal complaint"/>
 *                         &lt;enumeration value="legacy"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="Comment" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="Contract" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="PackageName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="Policy" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "id",
    "claimLineID",
    "idPayer",
    "memberID",
    "accumedPatientID",
    "payerID",
    "providerID",
    "emiratesIDNumber",
    "gross",
    "patientShare",
    "net",
    "providerInvoiceAmount",
    "denialCode",
    "paymentReference",
    "dateSettlement",
    "encounter",
    "diagnosis",
    "activity",
    "resubmission",
    "contract"
})
public class ObseleteClaim implements Serializable {

    @XmlElement(name = "ID", required = true, nillable = false)
    protected Long id;
    @XmlElement(name = "ClaimLineID")
    protected Long claimLineID;
    @XmlElement(name = "IDPayer")
    protected String idPayer;
    @XmlElement(name = "MemberID")
    protected String memberID;
    @XmlElement(name = "AccumedPatientID")
    protected Long accumedPatientID;
    @XmlElement(name = "PayerID", required = true)
    protected String payerID;
    @XmlElement(name = "ProviderID", required = true)
    protected String providerID;
    @XmlElement(name = "EmiratesIDNumber", required = true)
    protected String emiratesIDNumber;
    @XmlElement(name = "Gross")
    protected float gross;
    @XmlElement(name = "PatientShare")
    protected float patientShare;
    @XmlElement(name = "Net")
    protected float net;
    @XmlElement(name = "ProviderInvoiceAmount")
    protected float providerInvoiceAmount;
    @XmlElement(name = "DenialCode")
    protected String denialCode;
    @XmlElement(name = "PaymentReference")
    protected String paymentReference;
    //@XmlElement(name = "DateSettlement")
    @XmlElement(name = "DateSettlement", required = false)
    /*@XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)*/
    protected Date dateSettlement;
    @XmlElement(name = "Encounter", required = true)
    protected List<ObseleteEncounter> encounter;
    @XmlElement(name = "Diagnosis", required = true)
    protected List<ObseleteDiagnosis> diagnosis;
    @XmlElement(name = "Activity", required = true)
    protected List<ObseleteActivity> activity;

    @XmlElement(name = "Resubmission")
    /*protected Resubmission resubmission;
     @XmlElement(name = "Contract")
     protected Contract contract;*/
    /**
     * Gets the value of the id property.
     *
     * @return possible object is {@link String }
     *
     */
    public Long getID() {
        return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setID(Long value) {
        this.id = value;
    }

    /**
     * Gets the value of the accumedID property.
     *
     */
    public Long getClaimLineID() {
        return claimLineID;
    }

    /**
     * Sets the value of the accumedID property.
     *
     */
    public void setClaimLineID(Long value) {
        this.claimLineID = value;
    }

    /**
     * Gets the value of the idPayer property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getIDPayer() {
        return idPayer;
    }

    /**
     * Sets the value of the idPayer property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setIDPayer(String value) {
        this.idPayer = value;
    }

    /**
     * Gets the value of the memberID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getMemberID() {
        return memberID;
    }

    /**
     * Sets the value of the memberID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setMemberID(String value) {
        this.memberID = value;
    }

    /**
     * Gets the value of the accumedPatientID property.
     *
     * @return possible object is {@link Long }
     *
     */
    public Long getAccumedPatientID() {
        return accumedPatientID;
    }

    /**
     * Sets the value of the accumedPatientID property.
     *
     * @param value allowed object is {@link Long }
     *
     */
    public void setAccumedPatientID(Long value) {
        this.accumedPatientID = value;
    }

    /**
     * Gets the value of the payerID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getPayerID() {
        return payerID;
    }

    /**
     * Sets the value of the payerID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setPayerID(String value) {
        this.payerID = value;
    }

    /**
     * Gets the value of the providerID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getProviderID() {
        return providerID;
    }

    /**
     * Sets the value of the providerID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setProviderID(String value) {
        this.providerID = value;
    }

    /**
     * Gets the value of the emiratesIDNumber property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getEmiratesIDNumber() {
        return emiratesIDNumber;
    }

    /**
     * Sets the value of the emiratesIDNumber property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setEmiratesIDNumber(String value) {
        this.emiratesIDNumber = value;
    }

    /**
     * Gets the value of the gross property.
     *
     */
    public float getGross() {
        return gross;
    }

    /**
     * Sets the value of the gross property.
     *
     */
    public void setGross(float value) {
        this.gross = value;
    }

    /**
     * Gets the value of the patientShare property.
     *
     */
    public float getPatientShare() {
        return patientShare;
    }

    /**
     * Sets the value of the patientShare property.
     *
     */
    public void setPatientShare(float value) {
        this.patientShare = value;
    }

    /**
     * Gets the value of the net property.
     *
     */
    public float getNet() {
        return net;
    }

    /**
     * Sets the value of the net property.
     *
     */
    public void setNet(float value) {
        this.net = value;
    }

    /**
     * Gets the value of the providerInvoiceAmount property.
     *
     */
    public float getProviderInvoiceAmount() {
        return providerInvoiceAmount;
    }

    /**
     * Sets the value of the providerInvoiceAmount property.
     *
     */
    public void setProviderInvoiceAmount(float value) {
        this.providerInvoiceAmount = value;
    }

    /**
     * Gets the value of the denialCode property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getDenialCode() {
        return denialCode;
    }

    /**
     * Sets the value of the denialCode property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setDenialCode(String value) {
        this.denialCode = value;
    }

    /**
     * Gets the value of the paymentReference property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getPaymentReference() {
        return paymentReference;
    }

    /**
     * Sets the value of the paymentReference property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setPaymentReference(String value) {
        this.paymentReference = value;
    }

    /**
     * Gets the value of the dateSettlement property.
     *
     * @return possible object is {@link String }
     *
     */
    public Date getDateSettlement() {
        return dateSettlement;
    }

    /**
     * Sets the value of the dateSettlement property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setDateSettlement(Date value) {
        this.dateSettlement = value;
    }

    /**
     * Gets the value of the encounter property.
     *
     * <p>
     * This accessor method returns a reference to the live list, not a
     * snapshot. Therefore any modification you make to the returned list will
     * be present inside the JAXB object. This is why there is not a
     * <CODE>set</CODE> method for the encounter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getEncounter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
         * {@link ObseleteEncounter }
     *
     *
     */
    public List<ObseleteEncounter> getEncounter() {
        if (encounter == null) {
            encounter = new ArrayList<ObseleteEncounter>();
        }
        return this.encounter;
    }

    /**
     * Gets the value of the diagnosis property.
     *
     * <p>
     * This accessor method returns a reference to the live list, not a
     * snapshot. Therefore any modification you make to the returned list will
     * be present inside the JAXB object. This is why there is not a
     * <CODE>set</CODE> method for the diagnosis property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDiagnosis().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
         * {@link Claim.Diagnosis }
     *
     *
     */
    public List<ObseleteDiagnosis> getDiagnosis() {
        if (diagnosis == null) {
            diagnosis = new ArrayList<ObseleteDiagnosis>();
        }
        return this.diagnosis;
    }

    /**
     * Gets the value of the activity property.
     *
     * <p>
     * This accessor method returns a reference to the live list, not a
     * snapshot. Therefore any modification you make to the returned list will
     * be present inside the JAXB object. This is why there is not a
     * <CODE>set</CODE> method for the activity property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getActivity().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
         * {@link ObseleteActivity }
     *
     *
     */
    public List<ObseleteActivity> getActivity() {
        if (activity == null) {
            activity = new ArrayList<ObseleteActivity>();
        }
        return this.activity;
    }

    /**
     * Gets the value of the resubmission property.
     *
     * @return possible object is {@link Claim.Resubmission }
     *
     */
    /*public Resubmission getResubmission() {
     return resubmission;
     }*/
    /**
     * Sets the value of the resubmission property.
     *
     * @param value allowed object is {@link Claim.Resubmission }
     *
     */
    /*public void setResubmission(Resubmission value) {
     this.resubmission = value;
     }*/
    /**
     * Gets the value of the contract property.
     *
     * @return possible object is {@link Claim.Contract }
     *
     */
    /*public Contract getContract() {
     return contract;
     }*/
    /**
     * Sets the value of the contract property.
     *
     * @param value allowed object is {@link Claim.Contract }
     *
     */
    /*public void setContract(Contract value) {
     this.contract = value;
     }*/
    public void setActivity(List<ObseleteActivity> activity) {
        this.activity = activity;
    }

    public void setDiagnosis(List<ObseleteDiagnosis> diagnosis) {
        this.diagnosis = diagnosis;
    }

    public void setEncounter(List<ObseleteEncounter> encounter) {
        this.encounter = encounter;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setIdPayer(String idPayer) {
        this.idPayer = idPayer;
    }

    public Long getId() {
        return id;
    }

    public String getIdPayer() {
        return idPayer;
    }
}
