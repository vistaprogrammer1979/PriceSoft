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
public class XmlGenderAdapter extends XmlAdapter<String, String> {

    @Override
    public String marshal(String v) throws Exception {
        //SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        //return dateFormat.format(v);
        //String v = reader.getValue();
        if (v.compareToIgnoreCase("0") == 0 || v.compareToIgnoreCase("1") == 0 || v.compareToIgnoreCase("9") == 0) {
            return v;
        } else if (v.compareToIgnoreCase("M") == 0) {
            return "1";
        } else if (v.compareToIgnoreCase("F") == 0) {
            return "0";
        } else if (v.compareToIgnoreCase("U") == 0) {
            return "9";
        }
        return "9";
    }

    @Override
    public String unmarshal(String v) throws Exception {
        //SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        //return dateFormat.parse(v);
        if (v.compareToIgnoreCase("0") == 0 || v.compareToIgnoreCase("1") == 0 || v.compareToIgnoreCase("9") == 0) {
            return v;
        } else if (v.compareToIgnoreCase("M") == 0) {
            return "1";
        } else if (v.compareToIgnoreCase("F") == 0) {
            return "0";
        } else if (v.compareToIgnoreCase("U") == 0) {
            return "9";
        }
        return "9";
    }
}
