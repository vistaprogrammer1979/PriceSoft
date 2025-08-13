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
public class XmlDateTimeAdapter extends XmlAdapter<String, Date> {
    @Override
    public String marshal(Date v) throws Exception {
        //DateUtils.truncate
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        return dateFormat.format(v);
        //String s = dateFormat.format(v);
        //System.out.println("sDate = "+v+"---"+s);
        //return s;
    }

    @Override
    public Date unmarshal(String v) throws Exception {
       // Date ret =  dateFormat.parse(v);
       // if(ret == null)
       //     System.out.println("the returned date is null "+v);
       // return ret;
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        return dateFormat.parse(v);
    }
}
