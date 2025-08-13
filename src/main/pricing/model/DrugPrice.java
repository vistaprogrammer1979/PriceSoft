/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.util.Date;

/**
 *
 * @author smutlak
 */
public class DrugPrice {
    private String code;
    private Double package_Price_to_Public;
    private Double unit_Price_to_Public;
    private Date startDate;
    private Date endDate;
    private Integer regulator;
    private String dosage_Form;
    
    private Integer DiscountTier; 
    private Double Package_Markup;
    private Boolean UPP_SCOPE;
    private Boolean INCLUDED_THIQA;
    private Double Unit_Markup;

    public Double getUnit_Markup() {
        return Unit_Markup;
    }

    public void setUnit_Markup(Double Unit_Markup) {
        this.Unit_Markup = Unit_Markup;
    }

    
    public DrugPrice() {
    }

    public Double getPackage_Markup() {
        return Package_Markup;
    }

    public void setPackage_Markup(Double Package_Markup) {
        this.Package_Markup = Package_Markup;
    }

    public Boolean getUPP_SCOPE() {
        return UPP_SCOPE;
    }

    public void setUPP_SCOPE(Boolean UPP_SCOPE) {
        this.UPP_SCOPE = UPP_SCOPE;
    }

    public Boolean getINCLUDED_THIQA() {
        return INCLUDED_THIQA;
    }

    public void setINCLUDED_THIQA(Boolean INCLUDED_THIQA) {
        this.INCLUDED_THIQA = INCLUDED_THIQA;
    }

    public DrugPrice(String code, Double package_Price_to_Public, 
            Double unit_Price_to_Public, Date startDate, Date endDate,
            Integer regulator, String dosage_Form, Integer DiscountTier, 
            Double Package_Markup, Boolean UPP_SCOPE, Boolean INCLUDED_THIQA,Double Unit_Markup) {
        this.code = code;
        this.package_Price_to_Public = package_Price_to_Public;
        this.unit_Price_to_Public = unit_Price_to_Public;
        this.startDate = startDate;
        this.endDate = endDate;
        this.regulator = regulator;
        this.dosage_Form = dosage_Form;
        this.DiscountTier = DiscountTier;
        this.Package_Markup = Package_Markup;
        this.UPP_SCOPE = UPP_SCOPE;
        this.INCLUDED_THIQA = INCLUDED_THIQA;
        this.Unit_Markup=Unit_Markup;
    }

    public DrugPrice(String code, Double package_Price_to_Public, Double unit_Price_to_Public, Date startDate, Date endDate, Integer regulator,
            String dosage_Form,Integer DiscountTier ) {
        this.code = code;
        this.package_Price_to_Public = package_Price_to_Public;
        this.unit_Price_to_Public = unit_Price_to_Public;
        this.startDate = startDate;
        this.endDate = endDate;
        this.regulator = regulator;
        this.dosage_Form = dosage_Form;
        this.DiscountTier=DiscountTier;
    }
    public DrugPrice(String code, Double package_Price_to_Public, Double unit_Price_to_Public, Date startDate, Date endDate, Integer regulator,
            String dosage_Form) {
        this.code = code;
        this.package_Price_to_Public = package_Price_to_Public;
        this.unit_Price_to_Public = unit_Price_to_Public;
        this.startDate = startDate;
        this.endDate = endDate;
        this.regulator = regulator;
        this.dosage_Form = dosage_Form;
        
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getPackage_Price_to_Public() {
        return package_Price_to_Public;
    }

    public void setPackage_Price_to_Public(Double package_Price_to_Public) {
        this.package_Price_to_Public = package_Price_to_Public;
    }

    public Double getUnit_Price_to_Public() {
        return unit_Price_to_Public;
    }

    public void setUnit_Price_to_Public(Double unit_Price_to_Public) {
        this.unit_Price_to_Public = unit_Price_to_Public;
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

    public Integer getRegulator() {
        return regulator;
    }

    public void setRegulator(Integer regulator) {
        this.regulator = regulator;
    }

    public String getDosage_Form() {
        return dosage_Form;
    }

    public void setDosage_Form(String dosage_Form) {
        this.dosage_Form = dosage_Form;
    }
    public Integer getDiscountTier() {
        return DiscountTier;
    }

    public void setDiscountTier(Integer DiscountTier) {
        this.DiscountTier = DiscountTier;
    }

    
}
