/**
 * import this in the bolt file and declare an object obj. Then initialize 
 * the obj inside prepare method
 * and call obj.initializeMetricReporting().
 * And call obj.tuplesReceived.mark() where the tuple is completely processed
 * that is after emitting the tuple.
 */

package storm.ubiquitous.bolts;

/*For feeding data to graphite */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

class MetricForBolt{

    private static final String GRAPHITE_HOST = "127.0.0.1";
    private static final int CARBON_AGGREGATOR_LINE_RECEIVER_PORT = 2023;
    // The following value must match carbon-cache's storage-schemas.conf!
    private static final int GRAPHITE_REPORT_INTERVAL_IN_SECONDS = 10;
    private static final String GRAPHITE_METRICS_NAMESPACE_PREFIX = "production.apps.graphitedemo";
    private static final Pattern hostnamePattern = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");
    public transient Meter tuplesReceived;

    public void initializeMetricReporting() {
	final MetricRegistry registry = new MetricRegistry();
	final Graphite graphite = new Graphite(new InetSocketAddress(GRAPHITE_HOST,
								     CARBON_AGGREGATOR_LINE_RECEIVER_PORT));
	final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
	    .prefixedWith(metricsPath())
	    .convertRatesTo(TimeUnit.SECONDS)
	    .convertDurationsTo(TimeUnit.MILLISECONDS)
	    .filter(MetricFilter.ALL)
	    .build(graphite);
	reporter.start(GRAPHITE_REPORT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
	tuplesReceived = registry.meter(MetricRegistry.name("tuples", "received"));
    }

    private String metricsPath() {
	final String myHostname = extractHostnameFromFQHN(detectHostname());
	return GRAPHITE_METRICS_NAMESPACE_PREFIX + "." + myHostname;
    }

    private static String detectHostname() {
	String hostname = "hostname-could-not-be-detected";
	try {
	    hostname = InetAddress.getLocalHost().getHostName();
	}
	catch (UnknownHostException e) {
	    //LOG.error("Could not determine hostname");
	}
	return hostname;
    }

    private static String extractHostnameFromFQHN(String fqhn) {
	if (hostnamePattern.matcher(fqhn).matches()) {
	    if (fqhn.contains(".")) {
		return fqhn.split("\\.")[0];
	    }
	    else {
		return fqhn;
	    }
	}
	else {
	    // We want to return the input as-is
	    // when it is not a valid hostname/FQHN.
	    return fqhn;
	}
    }

}
