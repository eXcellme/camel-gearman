package org.apache.camel.component.gearman;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.RejectedExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.gearman.Gearman;
import org.gearman.GearmanClient;
import org.gearman.GearmanServer;

public class GearmanProducer extends DefaultProducer{

    private GearmanClient client ; 
    
    public GearmanProducer(GearmanEndpoint endpoint) {
        super(endpoint);
    }
    
    @Override
    protected void doStart() throws Exception {
        Gearman gearman = getEndpoint().getGearman();
        String host = getEndpoint().getHost();
        int port = getEndpoint().getPort();
        GearmanServer server = gearman.createGearmanServer(host, port);
        client = gearman.createGearmanClient();
        client.addServer(server);
    }

    @Override
    public GearmanEndpoint getEndpoint() {
        return (GearmanEndpoint)super.getEndpoint();
    }
    
    @Override
    public void process(Exchange exchange) {
        // deny processing if we are not started
        if (!isRunAllowed()) {
            if (exchange.getException() == null) {
                exchange.setException(new RejectedExecutionException());
            }
            // we cannot process so invoke callback
            return;
        }
        
//        System.out.println("gearman----producer----msg:"+exchange.getIn());
        String msg = exchange.getIn().getBody(String.class);
        try {
            client.submitBackgroundJob(getEndpoint().getFunctionName(),msg.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        
    }

}
