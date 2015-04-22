package org.elasticsearch.service.graphite;

import org.elasticsearch.common.base.MoreObjects;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.regex.Pattern;

public class GraphiteSettings {
    public static final TimeValue DEFAULT_REFRESH_INTERVAL = TimeValue.timeValueMinutes(1);
    public static final int DEFAULT_GRAPHITE_PORT = 2003;

    private TimeValue refreshInterval;
    private String host;
    private int port;
    private String prefix;
    private Pattern inclusionRegex;
    private Pattern exclusionRegex;

    public GraphiteSettings(final Settings settings) {
        Settings graphitePrefixed = settings.getByPrefix("metrics.graphite.");

        this.refreshInterval = graphitePrefixed.getAsTime("every", DEFAULT_REFRESH_INTERVAL);
        this.host = graphitePrefixed.get("host");
        this.port = graphitePrefixed.getAsInt("port", DEFAULT_GRAPHITE_PORT);
        this.prefix = graphitePrefixed.get("prefix", "elasticsearch" + '.' + settings.get("cluster.name"));
        String graphiteInclusionRegexString = graphitePrefixed.get("include");
        if (graphiteInclusionRegexString != null) {
            this.inclusionRegex = Pattern.compile(graphiteInclusionRegexString);
        }
        String graphiteExclusionRegexString = graphitePrefixed.get("exclude");
        if (graphiteExclusionRegexString != null) {
            this.exclusionRegex = Pattern.compile(graphiteExclusionRegexString);
        }
    }

    public TimeValue refreshInterval() {
        return refreshInterval;
    }

    public GraphiteSettings refreshInterval(String value) {
        this.refreshInterval = TimeValue.parseTimeValue(value, DEFAULT_REFRESH_INTERVAL);
        return this;
    }

    public String host() {
        return host;
    }

    public GraphiteSettings host(String host) {
        this.host = host;
        return this;
    }

    public int port() {
        return port;
    }

    public GraphiteSettings port(int port) {
        this.port = port;
        return this;
    }

    public String prefix() {
        return prefix;
    }

    public GraphiteSettings prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public Pattern inclusionRegex() {
        return inclusionRegex;
    }

    public GraphiteSettings inclusionRegex(String inclusionRegex) {
        this.inclusionRegex = null != inclusionRegex ? Pattern.compile(inclusionRegex) : null;
        return this;
    }

    public Pattern exclusionRegex() {
        return exclusionRegex;
    }

    public GraphiteSettings exclusionRegex(String exclusionRegex) {
        this.exclusionRegex = null != exclusionRegex ? Pattern.compile(exclusionRegex) : null;
        return this;
    }

    public boolean isHostDefined() {
        return !Strings.isNullOrEmpty(host);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .add("prefix", prefix)
                .add("refreshInterval", refreshInterval)
                .add("regex.inclusion", inclusionRegex)
                .add("regex.exclusion", inclusionRegex)
                .toString();
    }
}
