/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.webservices.converters;

import com.accumed.pricing.model.request.CodeType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
/**
 *
 * @author smutlak
 */
public class XmlCodeTypeAdapter extends XmlAdapter<Integer, CodeType> {
    @Override
    public Integer marshal(CodeType v) throws Exception {
        return v.getValue();
    }

    @Override
    public CodeType unmarshal(Integer v) throws Exception {
        return CodeType.from(v);
    }
}
