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
public enum Regulator {
    BOTH(0),
    HAAD(1),
    DHA(2),
    WHO(3),
    RIAYATI(4);

    private final int val;

    Regulator(int val) {
        this.val = val;
    }

    public int getValue() {
        return val;
    }

    // Mapping difficulty to difficulty id
    private static final Map<Integer, Regulator> _map = new HashMap<Integer, Regulator>();

    static {
        for (Regulator codeType : Regulator.values()) {
            _map.put(codeType.val, codeType);
        }
    }

    /**
     * Get difficulty from value
     *
     * @param value Value
     * @return Difficulty
     */
    public static Regulator from(int value) {
        return _map.get(value);
    }

}
