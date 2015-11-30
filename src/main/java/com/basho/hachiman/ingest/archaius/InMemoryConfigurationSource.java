package com.basho.hachiman.ingest.archaius;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;

@Component
public class InMemoryConfigurationSource implements PolledConfigurationSource {
    

    private Map<String, Object> settings = new HashMap<>();

    private synchronized Map<String, Object> readSettings() {
        return settings;
    }

    public synchronized void putSetting(String name, Object value) {
        settings.put(name, value);
    }

    @Override
    public PollResult poll(boolean initial, Object checkPoint) throws Exception {
        return PollResult.createFull(readSettings());
    }

}
