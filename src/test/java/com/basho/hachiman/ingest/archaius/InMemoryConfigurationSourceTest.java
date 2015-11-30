package com.basho.hachiman.ingest.archaius;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.basho.hachiman.ingest.IngestApplication;
import com.basho.hachiman.ingest.Pipeline;
import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.RiakConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicConfiguration;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.FixedDelayPollingScheduler;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IngestApplication.class)
public class InMemoryConfigurationSourceTest {

    private static InMemoryConfigurationSource source;
    private static FixedDelayPollingScheduler scheduler;
    
    @Autowired
    private ObjectMapper jsonMapper;

    @BeforeClass
    public static void setUp() throws Exception {
        source = new InMemoryConfigurationSource();
        scheduler = new FixedDelayPollingScheduler(10, 10, false);
        DynamicConfiguration dynConf = new DynamicConfiguration(source, scheduler);
        ConfigurationManager.install(dynConf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        scheduler.stop();
    }

    @Test
    public void testPutSetting() throws InterruptedException {
        DynamicStringProperty hello = DynamicPropertyFactory.getInstance().getStringProperty("hello", "world");
        assertEquals("world", hello.get());
        Thread.sleep(100);
        
        source.putSetting("hello", "1");
        Thread.sleep(100);
        assertEquals("1", hello.get());
        
        source.putSetting("hello", "2");
        Thread.sleep(100);
        assertEquals("2", hello.get());
    }
    
    
    @Test
    public void testPutJson() throws InterruptedException, IOException {
        Pipeline pipeline = new Pipeline("test", 
                new KafkaConfig("topic", Lists.newArrayList("khost1", "khost2")),
                new RiakConfig("bucket", Lists.newArrayList("rhost1", "rhost2")));
   
        String str = jsonMapper.writeValueAsString(pipeline);
        
//        System.out.println(str);
        
        source.putSetting("pipeline1", str);
        Thread.sleep(100);
        DynamicStringProperty prop = DynamicPropertyFactory.getInstance().getStringProperty("pipeline1", "");
        
        //prop.addCallback(callback);
        Pipeline p2 = jsonMapper.readValue(prop.get(), Pipeline.class);
        
        assertEquals(pipeline.getName(), p2.getName());
    }
}
