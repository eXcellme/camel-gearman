package org.apache.camel.component.gearman;

import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.gearman.Gearman;
import org.gearman.GearmanFunction;
import org.gearman.GearmanFunctionCallback;
import org.gearman.GearmanServer;
import org.gearman.GearmanWorker;

public class GearmanConsumer extends DefaultConsumer {

    private List<GearmanFuncionConsumer> funcs = new ArrayList<GearmanFuncionConsumer>();
    
    public GearmanConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor); 
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        
        Gearman gearman = getEndpoint().getGearman();
        String host = getEndpoint().getHost();
        int port = getEndpoint().getPort();
        String name = getEndpoint().getFunctionName();
        
        GearmanServer server = gearman.createGearmanServer(host, port);

        GearmanWorker worker = gearman.createGearmanWorker();
//        worker.setMaximumConcurrency();
        GearmanFuncionConsumer func = new GearmanFuncionConsumer(this);
        GearmanFunction function = func;
        worker.addFunction(name,function);

        worker.setMaximumConcurrency(getEndpoint().getWorkerThread());
        funcs.add(func);
        boolean b = worker.addServer(server);
        if(log.isDebugEnabled()){
            log.debug("gearman add server {}:{}/{} {}",new Object[]{host,port,name,b?"successful":"failed"});
        }
    }
    
    @Override
    public GearmanEndpoint getEndpoint() {
        return (GearmanEndpoint)super.getEndpoint();
    }
    
    
    @Override
    protected void doShutdown() throws Exception {
        super.doShutdown();
        getEndpoint().getGearman().shutdown();
    }
    @Override
    protected void doStop() throws Exception {
        super.doStop();
        getEndpoint().getGearman().shutdown();
    }
    
    class GearmanFuncionConsumer implements GearmanFunction{
        
        private GearmanConsumer consumer;
        public GearmanFuncionConsumer(GearmanConsumer consumer) {
            this.consumer = consumer;
        }
        @Override
        public byte[] work(String function, byte[] data, GearmanFunctionCallback callback) throws Exception {
            Exchange exchange = consumer.getEndpoint().createGearmanExchange(function,data);
            log.debug("gearman consuemr process before {}",exchange);
            consumer.getProcessor().process(exchange);
            log.debug("gearman consuemr process after {}",exchange);
            return "OK".getBytes();
        }
    }
    
}
