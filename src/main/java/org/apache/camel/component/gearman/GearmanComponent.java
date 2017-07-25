package org.apache.camel.component.gearman;

import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.UriEndpointComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**    
 * @Description Camel Gearman component
 * @author baofan.li
 * @Created 2016-1-14
 *
 */
public class GearmanComponent extends UriEndpointComponent{
    protected final Logger LOG = LoggerFactory.getLogger(GearmanComponent.class);

    public GearmanComponent() {
        super( GearmanEndpoint.class);
    }
    
    public GearmanComponent(CamelContext context) {
        super(context, GearmanEndpoint.class);
    }

    @Override
    protected GearmanEndpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        // eg. uri : gearman://192.168.3.197:4730:test
        // eg. remaining : 192.168.3.197:4730:test
//        LOG.debug("gearman createEndpoint uri:{} remaining:{} parameters:{}",uri,remaining,parameters);
        String[] args = remaining.split(":|/");
        String host = args[0];
        String port = args[1];
        String func = args[2];
        GearmanEndpoint endpoint = new GearmanEndpoint();
        endpoint.setHost(host);
        endpoint.setPort(Integer.parseInt(port));
        endpoint.setFunctionName(func);
        endpoint.init();
        return endpoint;
    }

}
