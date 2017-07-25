package org.apache.camel.component.gearman;

import java.io.IOException;
import org.apache.camel.Endpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.gearman.Gearman;
import org.gearman.GearmanClient;
import org.gearman.GearmanJobEvent;
import org.gearman.GearmanJobReturn;
import org.gearman.GearmanServer;
import org.junit.Test;

public class GearmanConsumerTest extends CamelTestSupport{
    
    private static final String FUNCTION_NAME = "test";
    // hosts add gearman.test
    @EndpointInject(uri = "gearman:gearman.test:4730/"+FUNCTION_NAME+"?charset=utf-8&workerThread=15&threadTimeout=3000")
    private Endpoint from;
    
    @EndpointInject(uri = "mock:result")
    private MockEndpoint to;
    
    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from(from).to(to);
            }
        };
    }
    
    @Test
    public void test() throws InterruptedException, IOException{
        String content = "this is a test ";
        to.expectedMessageCount(1);
        to.expectedHeaderReceived(GearmanConstants.FUNCTION_NAME, FUNCTION_NAME);
        to.expectedBodiesReceived(content);
        
        send(FUNCTION_NAME,content);
        to.assertIsSatisfied();
    }

    private void send(String function,String data) throws InterruptedException, IOException {
        Gearman gm = Gearman.createGearman();
        GearmanServer server = gm.createGearmanServer("gearman.test", 4730);
        GearmanClient client = gm.createGearmanClient();
        client.addServer(server);
        try {
            GearmanJobReturn jobResult = client.submitBackgroundJob(function, data.getBytes("utf-8"));
            while(!jobResult.isEOF()){
                GearmanJobEvent ev = jobResult.poll();
                String d = new String(ev.getData());
                System.out.printf("[ev data:%s type:%s]",d,ev.getEventType());
                
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e ;
        }finally{
            Thread.sleep(1000);
            server.shutdown();
            client.shutdown();
            gm.shutdown();
        }
    }
    
    
    
}
