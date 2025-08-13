/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.webservices.converters;


import javax.xml.bind.annotation.adapters.XmlAdapter;
/**
 *
 * @author smutlak
 */
public class XmlStringLength extends XmlAdapter<String, String> {

    @Override
    public String unmarshal(String v) throws Exception {
        if(v != null && v.length()>150){
            return v.substring(0, 150)+"...";
        }
        return v;
    }

    @Override
    public String marshal(String v) throws Exception {
        return v;
    }
    
}
