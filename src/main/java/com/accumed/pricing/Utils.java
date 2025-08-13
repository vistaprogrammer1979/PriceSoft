/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.ObjectFilter;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.conf.ConsequenceExceptionHandlerOption;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.ConsequenceExceptionHandler;
import org.drools.runtime.rule.FactHandle;
import org.drools.io.ResourceFactory;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import com.accumed.pricing.model.request.Claim;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *
 * @author smutlak
 */
public class Utils {

    public static KnowledgeBase createKnowledgeBase(String packageFilePath) {
        FileInputStream fis = null;
        KnowledgeBase kBase = null;
        Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Loading " + packageFilePath + " ...");
        try {
            File pkgFile = new File(packageFilePath);
            fis = new FileInputStream(pkgFile);
            KnowledgeBaseConfiguration kBaseConfig
                    = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
            @SuppressWarnings("unchecked")
            ConsequenceExceptionHandlerOption cehOption
                    = ConsequenceExceptionHandlerOption.get(
                            (Class<? extends ConsequenceExceptionHandler>) APEConsequenceExceptionHandler.class);

            kBaseConfig.setOption(cehOption);
            kBase = KnowledgeBaseFactory.newKnowledgeBase(kBaseConfig);

            KnowledgeBuilder kBuilder
                    = KnowledgeBuilderFactory.newKnowledgeBuilder();
            String drlPath = packageFilePath;
            if (drlPath != null) {
                //Resource drl = ResourceFactory.newClassPathResource(drlPath,
                //        getClass());
                kBuilder.add(ResourceFactory.newInputStreamResource(fis), ResourceType.PKG);
                //kBuilder.add(drl, ResourceType.DRL);
            }

            if (kBuilder.hasErrors()) {
                System.err.println("### compilation errors ###");
                KnowledgeBuilderErrors errors = kBuilder.getErrors();
                for (KnowledgeBuilderError err : errors) {
                    System.err.println(err.toString());
                }
                Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Failed");
                return null;
            }

            kBase.addKnowledgePackages(kBuilder.getKnowledgePackages());
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Done");
        } catch (FileNotFoundException ex) {
            Statistics.addException(ex);
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Failed");
            return null;
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
                Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Failed");
                return null;
            }
        }
        return kBase;
    }

    public static KnowledgeBase createKnowledgeBase1(String packageFilePath) {
        FileInputStream fis = null;
        Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Loading " + packageFilePath + " ...");

        try {
            File pkgFile = new File(packageFilePath);
            fis = new FileInputStream(pkgFile);
            KnowledgeBuilder builder = KnowledgeBuilderFactory.newKnowledgeBuilder();
            builder.add(ResourceFactory.newInputStreamResource(fis), ResourceType.PKG);
            if (builder.hasErrors()) {
                Logger.getLogger(Utils.class.getName()).log(Level.INFO, "{0}", builder.getErrors());
                return null;
            }

            KnowledgeBaseConfiguration kbaseConf = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
            @SuppressWarnings("unchecked")
            Class ehClass = (Class) APEConsequenceExceptionHandler.class;
            ConsequenceExceptionHandlerOption cehOption
                    = ConsequenceExceptionHandlerOption.get(ehClass);
            kbaseConf.setOption(cehOption);
            /*
             Class<? extends ConsequenceExceptionHandler> handler = APEConsequenceExceptionHandler.class;
             // setting the option using the type safe method
             Class<? extends ConsequenceExceptionHandler> handler;

             handler = (Class<? extends ConsequenceExceptionHandler>) APEConsequenceExceptionHandler.class;

             // Class handler = (Class)APEConsequenceExceptionHandler.class;
             kbaseConf.setOption(ConsequenceExceptionHandlerOption.get(handler));

            
             // setting the options using the string based setProperty() method
             kbaseConf.setProperty(ConsequenceExceptionHandlerOption.PROPERTY_NAME,
             handler.getName());*/

            // checking the type safe getOption() method
            KnowledgeBase base = KnowledgeBaseFactory.newKnowledgeBase(kbaseConf);
            base.addKnowledgePackages(builder.getKnowledgePackages());
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Done");

            return base;
        } catch (FileNotFoundException ex) {
            Statistics.addException(ex);
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Failed");
            return null;
        } finally {
            try {
                fis.close();
            } catch (IOException ex) {
                Statistics.addException(ex);
                Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
                Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Failed");
                return null;
            }
        }
    }

    public static String stackTraceToString(Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (e.getMessage() != null) {
            sb.append(e.getMessage());
            sb.append("\n");
        }
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String getRequest(com.accumed.pricing.model.request.Claim claim) {
        try {
            JAXBContext contextA = JAXBContext.newInstance(com.accumed.pricing.model.request.Claim.class);
            StringWriter writer = new StringWriter();
            javax.xml.bind.Marshaller marshaller = contextA.createMarshaller();
            marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, true);
            //marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FRAGMENT, true);
            marshaller.marshal(claim, writer);
            marshaller = null;

            return writer.toString();
        } catch (JAXBException e) {
            Statistics.addException(e);
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, e);
            return "Error Marshalling Request.";
        }
    }

    public static boolean saveRequest(Claim claim,boolean isRequestClaim,boolean requestLogging) {
       String sRequest = getRequest(claim);
    if (requestLogging || isRequestClaim) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-dd-MM-_HH-mm-ss-SSS");
        try {
            String dateString = dateFormat.format(new Date());
            String fileName = "";
            if (isRequestClaim) {
                fileName = "APM-" + claim.getIdCaller() + "_Response_";
            } else {
                fileName = "APM-" + claim.getIdCaller() + "_Request_";
            }
            // Specify the directory where you want to save the file (e.g., the system temp directory)
            String tempDir = System.getProperty("java.io.tmpdir");
            // Build the full file name without extra digits
            File file = new File(tempDir, fileName + "_" + dateString + ".log");
            
            // Optionally, if you want to avoid overwriting an existing file,
            // you may need to check if the file exists and modify the name accordingly.
            
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(sRequest);
            bw.close();
            
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Done");

        } catch (Exception e) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, e);
            return false;
        }
    }
    return true;
    }

    public static void deleteFacts(final StatefulKnowledgeSession session, final Class clss) {
        ObjectFilter filter = new ObjectFilter() {
            @Override
            public boolean accept(Object object) {
                return object.getClass().equals(clss) /*&& beanMatcher.matches(object,expectedProperties)*/;
            }
        };
        Collection<FactHandle> factHandles = session.getFactHandles(filter);
        for (FactHandle handle : factHandles) {
            session.retract(handle);
            Logger.getLogger(Utils.class.getName()).log(Level.INFO, "Sameer******old claims were found stacked in the sessions ....");
        }
    }
}
