/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import com.accumed.pricing.model.request.CodeType;

/**
 *
 * @author wfakhra
 */
public class DHA_DRG_COST_PER_ACTIVITY {

    private String code;
    private int BaseUnitsAnesthesia;
      private double       cost;
    private CodeType type;

    public DHA_DRG_COST_PER_ACTIVITY() {
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getBaseUnitsAnesthesia() {
        return BaseUnitsAnesthesia;
    }

    public void setBaseUnitsAnesthesia(int BaseUnitsAnesthesia) {
        this.BaseUnitsAnesthesia = BaseUnitsAnesthesia;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

    public DHA_DRG_COST_PER_ACTIVITY(String code,CodeType type, int BaseUnitsAnesthesia, double cost  ) {
        this.code = code;
        this.BaseUnitsAnesthesia = BaseUnitsAnesthesia;
        this.cost = cost;
        this.type = type;
    }

     
}
