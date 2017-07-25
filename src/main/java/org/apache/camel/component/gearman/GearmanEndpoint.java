package org.apache.camel.component.gearman;

import java.nio.charset.Charset;
import org.apache.camel.Exchange;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.Service;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.gearman.Gearman;
import org.gearman.context.GearmanContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gearman endpoint
 * baofan.li
 */
@ManagedResource(description = "Managed Gearman Endpoint")
@UriEndpoint(scheme = "gearman", title = "GEARMAN", syntax = "gearman:host:port/functionName", consumerClass = GearmanConsumer.class, label = "messaging")
public class GearmanEndpoint extends DefaultEndpoint implements MultipleConsumersSupport,Service{

    protected static final Logger LOG = LoggerFactory.getLogger(GearmanEndpoint.class);

    @UriPath(description = "gearman host") @Metadata(required = "true")
    private String host;
    @UriPath(description = "gearman port") @Metadata(required = "false",defaultValue="4730")
    private int port;
    @UriPath(description = "gearman function name") @Metadata(required = "true")
    private String functionName;

    @UriParam(defaultValue = "1", description = "gearman worker thread number")
    private int workerThread = 1;
    @UriParam(defaultValue = "30000", description = "gearman worker thread max idle time in pool ")
    private long threadTimeout = 30000;
    @UriParam(defaultValue = "utf-8", description = "charset")
    private String charset = "utf-8";
    @UriParam(defaultValue = "30000", description = "not used yet ")
    private int responseTimeout = 30000; // not used yet
    @UriParam(defaultValue = "60000", description = "not used yet ")
    private int pingTimeout = 60000;
    @UriParam(defaultValue = "60000", description = "not used yet ")
    private int idleTimeout = 60000;

    private Gearman gearman = null;
    
    private final GearmanMessageConverter messageConverter = new GearmanMessageConverter();
    
    public GearmanEndpoint() {
    }

    /**
     * init after parameter setup
     */
    void init(){
        if(this.threadTimeout > 0){
            LOG.debug("Camel Gearman parameter init threadTimeout: {}",this.threadTimeout);
            GearmanContext.setAttribute(GearmanContext.ATTRIBUTE_THREAD_TIMEOUT, this.threadTimeout);
        }
        if(this.workerThread > 0){
            LOG.debug("Camel Gearman parameter init workerThread: {}",this.workerThread);
            GearmanContext.setAttribute(GearmanContext.ATTRIBUTE_WORKER_THREADS, this.workerThread);
        }
        if(this.charset != null){
            LOG.debug("Camel Gearman parameter init charset: {}",this.charset);
            GearmanContext.setAttribute(GearmanContext.ATTRIBUTE_CHARSET, Charset.forName(this.charset));
        }
        gearman = GearmanSupport.getGearman();
    }
    
    @Override
    public Producer createProducer() throws Exception {
       return new GearmanProducer(this);
    }

    @Override
    public GearmanConsumer createConsumer(Processor processor) throws Exception {
        GearmanConsumer consumer = new GearmanConsumer(this, processor);
        return consumer;
    }

    @ManagedAttribute
    public boolean isSingleton() {
        return true ;
    }

    @Override
    public boolean isMultipleConsumersSupported() {
        return true;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }

    public long getThreadTimeout() {
        return threadTimeout;
    }

    public void setThreadTimeout(long threadTimeout) {
        this.threadTimeout = threadTimeout;
    }

    public int getPingTimeout() {
        return pingTimeout;
    }

    public void setPingTimeout(int pingTimeout) {
        this.pingTimeout = pingTimeout;
    }

    public int getResponseTimeout() {
        return responseTimeout;
    }

    public void setResponseTimeout(int responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public Gearman getGearman() {
        return gearman;
    }

    public Exchange createGearmanExchange(String function, byte[] data) {
        Exchange exchange = super.createExchange();
        messageConverter.populateGearmanExchange(exchange,function,data);
        return exchange;
    }
    @Override
    protected String createEndpointUri() {
        String scheme = "gearman";
        return scheme + ":" + host + ":" + port + "/" + functionName;  
    }
    
}
