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
public class DRGExcludedCpts {

    private String code;
    private CodeType type;

    public DRGExcludedCpts() {
    }

    public DRGExcludedCpts(String code, CodeType type) {
        this.code = code;
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

}
