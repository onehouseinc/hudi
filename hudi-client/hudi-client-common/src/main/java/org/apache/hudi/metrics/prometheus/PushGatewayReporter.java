/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics.prometheus;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.apache.hudi.metrics.HoodieGauge;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PushGatewayReporter extends ScheduledReporter {

  private static final Logger LOG = LogManager.getLogger(PushGatewayReporter.class);

  private final PushGateway pushGateway;
  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;
  private final String jobName;
  private final boolean deleteShutdown;

  protected PushGatewayReporter(MetricRegistry registry,
                                MetricFilter filter,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                String jobName,
                                String address,
                                boolean deleteShutdown) {
    super(registry, "hudi-push-gateway-reporter", filter, rateUnit, durationUnit);
    this.jobName = jobName;
    this.deleteShutdown = deleteShutdown;
    collectorRegistry = new CollectorRegistry();
    metricExports = new DropwizardExports(registry);
    pushGateway = new PushGateway(address);
    metricExports.register(collectorRegistry);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {

    if (gauges != null) {
      Map<String, String> groupingKey = null;
      String commitTime = null;
      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        String key = entry.getKey();
        Gauge value = entry.getValue();
        HoodieGauge hoodieGauge = (HoodieGauge) value;
        List<String> commitTimeTags = new ArrayList<String>(hoodieGauge.getTags().values());
        if (commitTimeTags.size() == 1) {
          if (commitTime == null) {
            commitTime = commitTimeTags.get(0);
            groupingKey = hoodieGauge.getTags();
          } else if (Long.parseLong(commitTime) < Long.parseLong(commitTimeTags.get(0))) {
            commitTime = commitTimeTags.get(0);
            groupingKey = hoodieGauge.getTags();
          }
        }
      }
      try {
        if (groupingKey != null) {
          pushGateway.pushAdd(collectorRegistry, jobName, groupingKey);
        } else {
          pushGateway.pushAdd(collectorRegistry, jobName);
        }
      } catch (IOException e) {
        LOG.warn("Can't push monitoring information to pushGateway", e);
      }
      return;
    }

    try {
      pushGateway.pushAdd(collectorRegistry, jobName);
    } catch (IOException e) {
      LOG.warn("Can't push monitoring information to pushGateway", e);
    }
  }

  @Override
  public void start(long period, TimeUnit unit) {
    super.start(period, unit);
  }

  @Override
  public void stop() {
    super.stop();
    try {
      if (deleteShutdown) {
        collectorRegistry.unregister(metricExports);
        pushGateway.delete(jobName);
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete metrics from pushGateway with jobName {" + jobName + "}", e);
    }
  }
}
