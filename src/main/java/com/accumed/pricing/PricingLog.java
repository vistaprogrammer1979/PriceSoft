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
public class PricingLog {
    private int claimId;
    private long periodInMilli;
    private long timestap;

    public PricingLog() {
    }

    public PricingLog(int claimId, long periodInMilli, long timestap) {
        this.claimId = claimId;
        this.periodInMilli = periodInMilli;
        this.timestap = timestap;
    }

    public int getClaimId() {
        return claimId;
    }

    public void setClaimId(int claimId) {
        this.claimId = claimId;
    }

    public long getPeriodInMilli() {
        return periodInMilli;
    }

    public void setPeriodInMilli(long periodInMilli) {
        this.periodInMilli = periodInMilli;
    }

    public long getTimestap() {
        return timestap;
    }

    public void setTimestap(long timestap) {
        this.timestap = timestap;
    }

   
}
