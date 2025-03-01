/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.seatunnel.core.starter.errorlimit;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.Counter;


public class ErrorRecordTracker {
    private final Counter errorCounter;
    private final Counter totalCounter;
    private final ErrorLimitConfig errorLimitConfig;
    
    public ErrorRecordTracker(MetricsContext metricsContext, ErrorLimitConfig config) {
        this.errorCounter = metricsContext.counter("error_records");
        this.totalCounter = metricsContext.counter("total_records");
        this.errorLimitConfig = config;
    }

    public void recordError() {
        errorCounter.inc();
        checkErrorLimit();
    }

    public void recordTotal() {
        totalCounter.inc();
    }

    private void checkErrorLimit() {
        if (!errorLimitConfig.isEnabled()) {
            return;
        }
        if (errorLimitConfig.getErrorLimitCount() != null
            && errorCounter.getCount() >= errorLimitConfig.getErrorLimitCount()) {
            throw new RuntimeException(
                String.format("Error record count %d exceeded limit %d", 
                    errorCounter.getCount(), 
                    errorLimitConfig.getErrorLimitCount()));
        }
        if (errorLimitConfig.getErrorLimitPercentage() != null
            && totalCounter.getCount() > 0) {
            double errorRate = (double) errorCounter.getCount() / totalCounter.getCount();
            if (errorRate > errorLimitConfig.getErrorLimitPercentage()) {
                throw new RuntimeException(
                    String.format("Error percentage %.2f%% exceeded limit %.2f%%", 
                        errorRate * 100,
                        errorLimitConfig.getErrorLimitPercentage() * 100));
            }
        }
    }
} 