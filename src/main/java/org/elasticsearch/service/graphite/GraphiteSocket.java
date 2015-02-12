package org.elasticsearch.service.graphite;

import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.regex.Pattern;

public class GraphiteSocket implements AutoCloseable {
    private static final Joiner METRIC_JOINER = Joiner.on('.').skipNulls();
    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteSocket.class.getName());

    private GraphiteSettings settings;
    private Socket socket;
    private Writer writer;

    public GraphiteSocket(GraphiteSettings settings) {
        this.settings = settings;
        try {
            socket = new Socket(settings.host(), settings.port());
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void send(String name, Object object, long timestamp) throws IOException {
        sendInternal(name, object, timestamp);
        flush();
    }

    private void sendInternal(String name, Object object, long timestamp) throws IOException {
        final String nameToSend = METRIC_JOINER.join(settings.prefix(), sanitizeString(name));
        final String valueToSend = extractValue(object);

        // check if this value is excluded
        Pattern exclusionRegex = settings.exclusionRegex();
        if (exclusionRegex != null && exclusionRegex.matcher(nameToSend).matches()) {
            Pattern inclusionRegex = settings.inclusionRegex();
            if (inclusionRegex == null || !inclusionRegex.matcher(nameToSend).matches()) {
                return;
            }
        }
        writer.write(nameToSend);
        writer.write(' ');
        writer.write(valueToSend);
        writer.write(' ');
        writer.write(Long.toString(timestamp));
        writer.write('\n');
    }

    private String extractValue(Object object) {
        String value;
        if (object instanceof Double) {
            value = String.format("%2.2f", (Double) object);
        } else if (object instanceof Integer) {
            value = String.format("%d", (Integer) object);
        } else {
            value = object.toString();
        }
        return value;
    }

    private String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                logger.error("Error while flushing writer:", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Error while closing socket:", e);
            }
        }
    }
}

