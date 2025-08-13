/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author smutlak
 */
public enum FacilityType {
    UNKNOWN(1),
    CENTER(2),
    CLINIC(3),
    DIAGNOSTIC_CENTRE(4),
    HOSPITAL(5),
    OFFICE(6),
    PHARMACY(7),
    POLYCLINIC(8),
    PROVISION_OF_HEALTH_SERVICE(9),
    REHABILITATION_CENTRE(10),
    STORE(11);

    
    private final int val;
    
    FacilityType(int val)
    {
        this.val = val;
    }
    
    public int getValue() { return val; }
    
    // Mapping difficulty to difficulty id
    private static final Map<Integer, FacilityType> _map = new HashMap<Integer, FacilityType>();
    static
    {
        for (FacilityType codeType : FacilityType.values())
            _map.put(codeType.val, codeType);
    }
 
    /**
     * Get difficulty from value
     * @param value Value
     * @return Difficulty
     */
    public static FacilityType from(int value)
    {
        return _map.get(value);
    }
    
}
