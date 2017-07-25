package org.apache.camel.component.gearman;

import java.io.UnsupportedEncodingException;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GearmanMessageConverter {
    protected static final Logger LOG = LoggerFactory.getLogger(GearmanMessageConverter.class);

    public void populateGearmanExchange(Exchange exchange, String function, byte[] data) {
        // in message
        Message message = exchange.getIn();
        if(message == null){
            message = new DefaultMessage();
            exchange.setIn(message);
        }
        
        populateHeaders(message,function);
        populateBody(message,data);
        
    }

    private void populateHeaders(Message message, String function) {
        message.setHeader(GearmanConstants.FUNCTION_NAME, function);
    }

    private void populateBody(Message message , byte[] data) {
        // body 
        String body = null;
        try {
            body = new String(data,"utf-8");
        } catch (UnsupportedEncodingException e) {
            body = new String(data);
        }
        message.setBody(body);
    }
    
    
    
}
