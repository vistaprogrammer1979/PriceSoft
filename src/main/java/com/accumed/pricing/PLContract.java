/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

/**
 *
 * @author smutlak
 */
public class PLContract {

    Integer id;
    Integer type;
    Double baseRate;
    Double gap;
    Double marginal;
    Double IP_DRG_Factor;
    Double DayCase_DRG_FACTOR;
    String packageName;

    public PLContract(Integer id, Integer type, Double baseRate, Double gap, Double marginal, Double iP_DRG_Factor,Double DayCase_DRG_FACTOR,
            String packageName) {
        this.id = id;
        this.type = type;
        this.baseRate = baseRate;
        this.gap = gap;
        this.marginal = marginal;
        this.packageName = packageName;
        //Updated by vpande on 02 Feb 2023 to correct the variable name from IP_DRG_Factor to iP_DRG_Factor for Req#4
        this.IP_DRG_Factor = iP_DRG_Factor;
        //Updated by vpande on 02 Feb 2023 to correct the variable name from IP_DRG_Factor to iP_DRG_Factor for Req#4
        this.DayCase_DRG_FACTOR=DayCase_DRG_FACTOR;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Double getBaseRate() {
        return baseRate;
    }

    public void setBaseRate(Double baseRate) {
        this.baseRate = baseRate;
    }

    public Double getGap() {
        return gap;
    }

    public void setGap(Double gap) {
        this.gap = gap;
    }

    public Double getMarginal() {
        return marginal;
    }

    public void setMarginal(Double marginal) {
        this.marginal = marginal;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public Double getIP_DRG_Factor() {
        return IP_DRG_Factor;
    }

    public void setIP_DRG_Factor(Double IP_DRG_Factor) {
        this.IP_DRG_Factor = IP_DRG_Factor;
    }

    public Double getDayCase_DRG_FACTOR() {
        return DayCase_DRG_FACTOR;
    }

    public void setDayCase_DRG_FACTOR(Double DayCase_DRG_FACTOR) {
        this.DayCase_DRG_FACTOR = DayCase_DRG_FACTOR;
    }

    
   
    

    
    
}
