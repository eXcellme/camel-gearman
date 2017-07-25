package org.apache.camel.component.gearman;

import org.gearman.Gearman;

public class GearmanSupport {
    private static final Gearman GEARMAN_INSTANCE = Gearman.createGearman();
    static Gearman getGearman(){
        return GEARMAN_INSTANCE;
    }
}
