/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.webservices.converters;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.adapters.XmlAdapter;
/**
 *
 * @author smutlak
 */
public class XmlDateAdapter extends XmlAdapter<String, Date> {
    @Override
    public String marshal(Date v) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        return dateFormat.format(v);
    }

    @Override
    public Date unmarshal(String v) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        return dateFormat.parse(v);
    }
}
