/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.math.BigInteger;
import java.util.Date;
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
 *         &lt;element name="AccumedID" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="ID" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="Start">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *               &lt;minLength value="0"/>
 *               &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="Type">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *               &lt;enumeration value="1"/>
 *               &lt;enumeration value="2"/>
 *               &lt;enumeration value="3"/>
 *               &lt;enumeration value="4"/>
 *               &lt;enumeration value="5"/>
 *               &lt;enumeration value="6"/>
 *               &lt;enumeration value="8"/>
 *               &lt;enumeration value="9"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="Code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Quantity" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="Net" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="Clinician" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="PriorAuthorizationID" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="List" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *         &lt;element name="Gross" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *         &lt;element name="PatientShare" type="{http://www.w3.org/2001/XMLSchema}float" minOccurs="0"/>
 *         &lt;element name="PaymentAmount" type="{http://www.w3.org/2001/XMLSchema}float"/>
 *         &lt;element name="DenialCode" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="Observation" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="Type">
 *                     &lt;simpleType>
 *                       &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                         &lt;enumeration value="CPT"/>
 *                         &lt;enumeration value="HL7v3 Native"/>
 *                         &lt;enumeration value="LOINC"/>
 *                         &lt;enumeration value="SNOMED CT"/>
 *                         &lt;enumeration value="Text"/>
 *                         &lt;enumeration value="File"/>
 *                         &lt;enumeration value="Universal Dental"/>
 *                       &lt;/restriction>
 *                     &lt;/simpleType>
 *                   &lt;/element>
 *                   &lt;element name="Code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="Value" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *                   &lt;element name="ValueType" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
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
    "accumedID",
    "id",
    "start",
    "type",
    "code",
    "quantity",
    "net",
    "providerNet",
    "clinician",
    "priorAuthorizationID",
    "list",
    "gross",
    "patientShare",
    "paymentAmount",
    "denialCode",
    "observation"
})
public class ObseleteActivity {

    @XmlElement(name = "AccumedID")
    protected long accumedID;
    @XmlElement(name = "ID")
    protected String id;
    @XmlElement(name = "Start", required = true)
    /*@XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)*/
    protected Date start;
    @XmlElement(name = "Type", required = true)
    protected BigInteger type;
    @XmlElement(name = "Code", required = true)
    protected String code;
    @XmlElement(name = "Quantity")
    protected float quantity;
    @XmlElement(name = "Net")
    protected float net;
    @XmlElement(name = "ProviderNet")
    protected float providerNet;
    @XmlElement(name = "Clinician", required = true)
    protected String clinician;
    @XmlElement(name = "PriorAuthorizationID")
    protected String priorAuthorizationID;
    @XmlElement(name = "List")
    protected Float list;
    @XmlElement(name = "Gross")
    protected Float gross;
    @XmlElement(name = "PatientShare")
    protected Float patientShare;
    @XmlElement(name = "PaymentAmount")
    protected float paymentAmount;
    @XmlElement(name = "DenialCode")
    protected String denialCode;

    @XmlElement(name = "Observation")
    /*protected List<Observation> observation;*/
    /**
     * Gets the value of the accumedID property.
     *
     */
    public long getAccumedID() {
        return accumedID;
    }

    /**
     * Sets the value of the accumedID property.
     *
     */
    public void setAccumedID(long value) {
        this.accumedID = value;
    }

    /**
     * Gets the value of the id property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getID() {
        return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setID(String value) {
        this.id = value;
    }

    /**
     * Gets the value of the start property.
     *
     * @return possible object is {@link String }
     *
     */
    public Date getStart() {
        return start;
    }

    /**
     * Sets the value of the start property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setStart(Date value) {
        this.start = value;
    }

    /**
     * Gets the value of the type property.
     *
     * @return possible object is {@link BigInteger }
     *
     */
    public BigInteger getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     *
     * @param value allowed object is {@link BigInteger }
     *
     */
    public void setType(BigInteger value) {
        this.type = value;
    }

    /**
     * Gets the value of the code property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getCode() {
        return code;
    }

    /**
     * Sets the value of the code property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setCode(String value) {
        this.code = value;
    }

    /**
     * Gets the value of the quantity property.
     *
     */
    public float getQuantity() {
        return quantity;
    }

    /**
     * Sets the value of the quantity property.
     *
     */
    public void setQuantity(float value) {
        this.quantity = value;
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
     * Gets the value of the providerNet property.
     *
     */
    public float getProviderNet() {
        return providerNet;
    }

    /**
     * Sets the value of the providerNet property.
     *
     */
    public void setProviderNet(float value) {
        this.providerNet = value;
    }

    /**
     * Gets the value of the clinician property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getClinician() {
        return clinician;
    }

    /**
     * Sets the value of the clinician property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setClinician(String value) {
        this.clinician = value;
    }

    /**
     * Gets the value of the priorAuthorizationID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getPriorAuthorizationID() {
        return priorAuthorizationID;
    }

    /**
     * Sets the value of the priorAuthorizationID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setPriorAuthorizationID(String value) {
        this.priorAuthorizationID = value;
    }

    /**
     * Gets the value of the list property.
     *
     * @return possible object is {@link Float }
     *
     */
    public Float getList() {
        return list;
    }

    /**
     * Sets the value of the list property.
     *
     * @param value allowed object is {@link Float }
     *
     */
    public void setList(Float value) {
        this.list = value;
    }

    /**
     * Gets the value of the gross property.
     *
     * @return possible object is {@link Float }
     *
     */
    public Float getGross() {
        return gross;
    }

    /**
     * Sets the value of the gross property.
     *
     * @param value allowed object is {@link Float }
     *
     */
    public void setGross(Float value) {
        this.gross = value;
    }

    /**
     * Gets the value of the patientShare property.
     *
     * @return possible object is {@link Float }
     *
     */
    public Float getPatientShare() {
        return patientShare;
    }

    /**
     * Sets the value of the patientShare property.
     *
     * @param value allowed object is {@link Float }
     *
     */
    public void setPatientShare(Float value) {
        this.patientShare = value;
    }

    /**
     * Gets the value of the paymentAmount property.
     *
     */
    public float getPaymentAmount() {
        return paymentAmount;
    }

    /**
     * Sets the value of the paymentAmount property.
     *
     */
    public void setPaymentAmount(float value) {
        this.paymentAmount = value;
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
     * Gets the value of the observation property.
     *
     * <p>
     * This accessor method returns a reference to the live list, not a
     * snapshot. Therefore any modification you make to the returned list will
     * be present inside the JAXB object. This is why there is not a
     * <CODE>set</CODE> method for the observation property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getObservation().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
             * {@link Activityvation }
     *
     *
     */
    /*public List<Observation> getObservation() {
     if (observation == null) {
     observation = new ArrayList<Observation>();
     }
     return this.observation;
     }*/
    public void setId(String id) {
        this.id = id;
    }

    /*public void setObservation(List<Observation> observation) {
     this.observation = observation;
     }*/
}
