/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import javax.xml.ws.WebFault;

/**
 *
 * @author smutlak
 */
@WebFault(name="InvalidArgument")
public class InvalidArgument extends Exception{
    

    

    public InvalidArgument(String msg) {
        super(msg);
        
    }
}
