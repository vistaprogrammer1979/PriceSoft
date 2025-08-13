/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

/**
 *
 * @author smutlak
 */
public class GroupCodesRange {

    private String codeFrom;
    private String codeTo;

    public GroupCodesRange(String codeFrom, String codeTo) {
        this.codeFrom = codeFrom;
        this.codeTo = codeTo;
    }

    public String getCodeFrom() {
        return codeFrom;
    }

    public void setCodeFrom(String codeFrom) {
        this.codeFrom = codeFrom;
    }

    public String getCodeTo() {
        return codeTo;
    }

    public void setCodeTo(String codeTo) {
        this.codeTo = codeTo;
    }

    public boolean containsCode(String sCode) {
        if (sCode.equalsIgnoreCase(codeFrom)) {
            return true;
        }
        if (codeTo != null && codeTo.length() > 0) {
            if (sCode.compareToIgnoreCase(codeFrom) >= 0 && sCode.compareToIgnoreCase(codeTo) <= 0) {
                return true;
            }
        }
        return false;
    }

}
