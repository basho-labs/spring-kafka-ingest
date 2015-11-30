package com.basho.hachiman.ingest.archaius;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicConfiguration;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.FixedDelayPollingScheduler;
import com.netflix.config.PolledConfigurationSource;

@Component
public class DynamicPropertyFactoryProvider implements
        FactoryBean<DynamicPropertyFactory> {

    @Value("${hachiman.ingest.checkСonfigRateMs}")
    private int checkConfigRateMs;

    @Autowired
    private PolledConfigurationSource source;

    private FixedDelayPollingScheduler scheduler;

    @PostConstruct
    private void init() {
        scheduler = new FixedDelayPollingScheduler(0, checkConfigRateMs, false);
        DynamicConfiguration dynConf = new DynamicConfiguration(source, scheduler);
        ConfigurationManager.install(dynConf);
    }
    
    @PreDestroy
    public void shutdown() {
        scheduler.stop();
    }

    @Override
    public DynamicPropertyFactory getObject() throws Exception {
        return DynamicPropertyFactory.getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return DynamicPropertyFactory.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
