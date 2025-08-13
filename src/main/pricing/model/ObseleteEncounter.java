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
 *         &lt;element name="FacilityID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Type">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *               &lt;enumeration value="1"/>
 *               &lt;enumeration value="2"/>
 *               &lt;enumeration value="3"/>
 *               &lt;enumeration value="4"/>
 *               &lt;enumeration value="5"/>
 *               &lt;enumeration value="6"/>
 *               &lt;enumeration value="7"/>
 *               &lt;enumeration value="8"/>
 *               &lt;enumeration value="9"/>
 *               &lt;enumeration value="12"/>
 *               &lt;enumeration value="13"/>
 *               &lt;enumeration value="15"/>
 *               &lt;enumeration value="41"/>
 *               &lt;enumeration value="42"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="PatientID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Start">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *               &lt;minLength value="0"/>
 *               &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="End" minOccurs="0">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *               &lt;minLength value="0"/>
 *               &lt;pattern value="\d{2}/\d{2}/\d{4} (20|21|22|23|[0-1]?\d):[0-5]?\d"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="StartType" minOccurs="0">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *               &lt;enumeration value="1"/>
 *               &lt;enumeration value="2"/>
 *               &lt;enumeration value="3"/>
 *               &lt;enumeration value="4"/>
 *               &lt;enumeration value="5"/>
 *               &lt;enumeration value="6"/>
 *               &lt;enumeration value="7"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="EndType" minOccurs="0">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}integer">
 *               &lt;enumeration value="1"/>
 *               &lt;enumeration value="2"/>
 *               &lt;enumeration value="3"/>
 *               &lt;enumeration value="4"/>
 *               &lt;enumeration value="5"/>
 *               &lt;enumeration value="6"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="TransferSource" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="TransferDestination" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
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
    "facilityID",
    "type",
    "patientID",
    "start",
    "end",
    "startType",
    "endType",
    "transferSource",
    "transferDestination",
    "authorisation"
})
public class ObseleteEncounter {

    @XmlElement(name = "AccumedID")
    protected long accumedID;
    @XmlElement(name = "FacilityID", required = true)
    protected String facilityID;
    @XmlElement(name = "Type", required = true)
    protected BigInteger type;
    @XmlElement(name = "PatientID", required = true)
    protected String patientID;
    //@XmlElement(name = "Start", required = true)
    @XmlElement(name = "Start", required = false)
    /*@XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)*/
    protected Date start;
    //@XmlElement(name = "End")
    @XmlElement(name = "End", required = false)
    /*@XmlJavaTypeAdapter(com.accumed.webservices.converters.XmlDateTimeAdapter.class)*/
    protected Date end;
    @XmlElement(name = "StartType")
    protected BigInteger startType;
    @XmlElement(name = "EndType")
    protected BigInteger endType;
    @XmlElement(name = "TransferSource")
    protected String transferSource;
    @XmlElement(name = "TransferDestination")
    protected String transferDestination;

    /*@XmlElement(name = "Authorisation")
     protected Authorisation authorisation;*/
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
     * Gets the value of the facilityID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getFacilityID() {
        return facilityID;
    }

    /**
     * Sets the value of the facilityID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setFacilityID(String value) {
        this.facilityID = value;
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
     * Gets the value of the patientID property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getPatientID() {
        return patientID;
    }

    /**
     * Sets the value of the patientID property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setPatientID(String value) {
        this.patientID = value;
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
     * Gets the value of the end property.
     *
     * @return possible object is {@link String }
     *
     */
    public Date getEnd() {
        return end;
    }

    /**
     * Sets the value of the end property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setEnd(Date value) {
        this.end = value;
    }

    /**
     * Gets the value of the startType property.
     *
     * @return possible object is {@link BigInteger }
     *
     */
    public BigInteger getStartType() {
        return startType;
    }

    /**
     * Sets the value of the startType property.
     *
     * @param value allowed object is {@link BigInteger }
     *
     */
    public void setStartType(BigInteger value) {
        this.startType = value;
    }

    /**
     * Gets the value of the endType property.
     *
     * @return possible object is {@link BigInteger }
     *
     */
    public BigInteger getEndType() {
        return endType;
    }

    /**
     * Sets the value of the endType property.
     *
     * @param value allowed object is {@link BigInteger }
     *
     */
    public void setEndType(BigInteger value) {
        this.endType = value;
    }

    /**
     * Gets the value of the transferSource property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getTransferSource() {
        return transferSource;
    }

    /**
     * Sets the value of the transferSource property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setTransferSource(String value) {
        this.transferSource = value;
    }

    /**
     * Gets the value of the transferDestination property.
     *
     * @return possible object is {@link String }
     *
     */
    public String getTransferDestination() {
        return transferDestination;
    }

    /**
     * Sets the value of the transferDestination property.
     *
     * @param value allowed object is {@link String }
     *
     */
    public void setTransferDestination(String value) {
        this.transferDestination = value;
    }

    /*public Authorisation getAuthorisation() {
     return authorisation;
     }

     public void setAuthorisation(Authorisation authorisation) {
     this.authorisation = authorisation;
     }*/
}
