package com.example.processors.weather;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@SupportsBatching
@Tags({"http", "https", "rest", "client"})
@CapabilityDescription("An HTTP client processor which can interact with a configurable HTTP Endpoint. The destination URL and HTTP Method are configurable.")
public final class WeatherProcessor extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("Remote URL")
            .description("Remote URL which will be connected to, including scheme, host, port, path.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to remote service.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Max wait time for response from remote service.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private static final String RFC_1123 = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(RFC_1123).withLocale(Locale.US).withZoneUTC();
    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_URL);
        descriptors.add(PROP_CONNECT_TIMEOUT);
        descriptors.add(PROP_READ_TIMEOUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private volatile Pattern regexAttributesToSend = null;

    @OnScheduled
    public void setUpClient(final ProcessContext context) {
        okHttpClientAtomicReference.set(null);
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();
        okHttpClientBuilder.connectTimeout((context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(context.getProperty(PROP_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);
        okHttpClientAtomicReference.set(okHttpClientBuilder.build());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();
        FlowFile requestFlowFile = session.get();
        FlowFile responseFlowFile;
        try {
            final URL url = new URL(context.getProperty(PROP_URL).getValue());
            Request httpRequest = configureRequest(requestFlowFile, url);
            final long startNanos = System.nanoTime();
            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
                ResponseBody responseBody = responseHttp.body();
                InputStream responseBodyStream = null;
                try {
                    assert responseBody != null;
                    responseBodyStream = responseBody.byteStream();
                    responseFlowFile = session.create();
                    responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), Objects.requireNonNull(responseBody.contentType()).toString());
                    responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);

                } finally {
                    if (responseBodyStream != null) {
                        responseBodyStream.close();
                    }
                }
                session.transfer(responseFlowFile, SUCCESS);
            }
        } catch (final Exception e) {
            context.yield();
        }
    }

    private Request configureRequest(final FlowFile requestFlowFile, URL url) {
        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder = requestBuilder.url(url);
        requestBuilder = requestBuilder.get();
        requestBuilder = setHeaderProperties(requestBuilder, requestFlowFile);
        return requestBuilder.build();
    }

    private Request.Builder setHeaderProperties(Request.Builder requestBuilder, final FlowFile requestFlowFile) {
        requestBuilder = requestBuilder.addHeader("Date", DATE_FORMAT.print(System.currentTimeMillis()));

        if (regexAttributesToSend != null && requestFlowFile != null) {
            Map<String, String> attributes = requestFlowFile.getAttributes();
            Matcher m = regexAttributesToSend.matcher("");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String headerKey = trimToEmpty(entry.getKey());

                m.reset(headerKey);
                if (m.matches()) {
                    String headerVal = trimToEmpty(entry.getValue());
                    requestBuilder = requestBuilder.addHeader(headerKey, headerVal);
                }
            }
        }
        return requestBuilder;
    }
}
