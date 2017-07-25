package org.apache.camel.component.gearman;

import java.util.Map;
import org.apache.camel.CamelContext;
import org.mockito.Mockito;

public class GearmanComponentTest {
    private CamelContext context = Mockito.mock(CamelContext.class);
    
    private GearmanEndpoint createEndpoint(Map<String,Object> params) throws Exception{
        String uri = "gearman:gearman.test:4730/test";
        String remaining = "gearman.test:4730/test";
        return new GearmanComponent(context).createEndpoint(uri, remaining, params);
    }
    
    
}
