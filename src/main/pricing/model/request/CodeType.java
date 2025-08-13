/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model.request;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author wfakhra
 */
public enum CodeType {
    CPT(3),
    HCPCS(4),
    TRADE_DRUG(5),
    DENTAL(6),
    SERVICE(8),
    IR_DRG(9),
    GENERIC_DRUG(10),
    ICD9(17),
    ICD10(16),
    Package(18),
    CustomCode(19) ;
    
    private final int val;
    
    CodeType(int val)
    {
        this.val = val;
    }
    
    public int getValue() { return val; }
    
    // Mapping difficulty to difficulty id
    private static final Map<Integer, CodeType> _map = new HashMap<Integer, CodeType>();
    static
    {
        for (CodeType codeType : CodeType.values())
            _map.put(codeType.val, codeType);
    }
 
    /**
     * Get difficulty from value
     * @param value Value
     * @return Difficulty
     */
    public static CodeType from(int value)
    {
        return _map.get(value);
    }
    
    private boolean test(){
        return CodeType.HCPCS.getValue() == 4;
    }
    
}
