package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.riak.StringToRowFunction;
import com.basho.riak.client.core.query.timeseries.Cell;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by tmatvienko on 12/15/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = IngestApplication.class)
public class StringToRowFunctionUnitTest {

    @Autowired
    private StringToRowFunction stringToRowFunction;

    @Test
    public void canParse2015Row() {
        String msg = "[\"RG3\", \"WSPD\", \"1425027600000\", \"51.142082\", \"-0.194181\", \"0.8\"]";
        List<Cell> cells = stringToRowFunction.call(msg).getCells();
        assertFalse(cells.isEmpty());
        assertEquals(cells.size(), 9);
        assertTrue(cells.get(0).getVarcharAsUTF8String().equals("1"));
        assertTrue(cells.get(1).getVarcharAsUTF8String().equals("f"));
        assertNotNull(cells.get(2).getTimestamp());
        assertTrue(cells.get(3).getVarcharAsUTF8String().equals("RG3"));
        assertTrue(cells.get(4).getVarcharAsUTF8String().equals("WSPD"));
        assertTrue(cells.get(5).getTimestamp() == 1425027600000L);
        assertTrue(cells.get(6).getDouble() == 51.142082);
        assertTrue(cells.get(7).getDouble() == -0.194181);
        assertTrue(cells.get(8).getDouble() == 0.8);
    }
}
