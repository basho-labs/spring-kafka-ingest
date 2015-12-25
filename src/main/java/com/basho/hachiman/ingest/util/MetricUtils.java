package com.basho.hachiman.ingest.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.springframework.stereotype.Component;

/**
 * Created by tmatvienko on 12/25/15.
 */
@Component
public class MetricUtils {

    private static final String ERROR_COUNT = "hachiman.ingest.errorCount";
    private static final String MSG_COUNT   = "hachiman.ingest.messageCount";

    private final Counter msgCounter;
    private final Counter errCounter;

    public MetricUtils() {
        MetricRegistry registry = new MetricRegistry();
        this.msgCounter = registry.counter(MSG_COUNT);
        this.errCounter = registry.counter(ERROR_COUNT);
    }

    public long incMsgCount() {
        return incMsgCount(1);
    }

    public long incMsgCount(long cnt) {
        msgCounter.inc(cnt);
        return msgCounter.getCount();
    }

    public void incErrCount() {
        errCounter.inc();
    }

    public long getMsgCount() {
        return msgCounter.getCount();
    }

    public long getErrCount() {
        return errCounter.getCount();
    }

    public void resetErrCount() {
        errCounter.inc(-1L);
    }
}
