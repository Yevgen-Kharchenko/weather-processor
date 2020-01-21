/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.processors.weather;

import okhttp3.*;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@Tags({"JSON", "API"})
@CapabilityDescription("Fetch  json from path")
@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="weatherprocessor.status.message", description="The status message that is returned")})
@WritesAttributes({
        @WritesAttribute(attribute = "weatherprocessor.status.code", description = "The status code that is returned"),
        @WritesAttribute(attribute = "weatherprocessor.status.message", description = "The status message that is returned"),
        @WritesAttribute(attribute = "weatherprocessor.response.body", description = "In the instance where the status code received is not a success (2xx) "
                + "then the response body will be put to the 'weatherprocessor.response.body' attribute of the request FlowFile."),
        @WritesAttribute(attribute = "weatherprocessor.request.url", description = "The request URL"),
        @WritesAttribute(attribute = "weatherprocessor.tx.id", description = "The transaction ID that is returned after reading the response"),
        @WritesAttribute(attribute = "weatherprocessor.remote.dn", description = "The DN of the remote server"),
        @WritesAttribute(attribute = "weatherprocessor.java.exception.class", description = "The Java exception class raised when the processor fails"),
        @WritesAttribute(attribute = "weatherprocessor.java.exception.message", description = "The Java exception message raised when the processor fails")})

public class WeatherProcessor extends AbstractProcessor {

    public final static String STATUS_CODE = "weatherprocessor.status.code";
    public final static String STATUS_MESSAGE = "weatherprocessor.status.message";
    public final static String RESPONSE_BODY = "weatherprocessor.response.body";
    public final static String REQUEST_URL = "weatherprocessor.request.url";
    public final static String TRANSACTION_ID = "weatherprocessor.tx.id";
    public final static String REMOTE_DN = "weatherprocessor.remote.dn";
    public final static String EXCEPTION_CLASS = "weatherprocessor.java.exception.class";
    public final static String EXCEPTION_MESSAGE = "weatherprocessor.java.exception.message";

    public static final Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            STATUS_CODE)));

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("Remote URL")
            .description("Remote URL which will be connected to, including scheme, host, port, path.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_URL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();
        FlowFile requestFlowFile = session.get();
        if (requestFlowFile == null) {
            return;
        } else requestFlowFile = session.create();

//        final ComponentLog logger = getLogger();
//        final UUID txId = UUID.randomUUID();
//
//        FlowFile responseFlowFile = null;
//        try {
//            final String urlstr = trimToEmpty(context.getProperty(PROP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());
//            final URL url = new URL(urlstr);
//
//            Request httpRequest = configureRequest(url);
//
//            logRequest(logger, httpRequest);
//
//            if (httpRequest.body() != null) {
//                session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
//            }
//
//            final long startNanos = System.nanoTime();
//
//            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
//                // output the raw response headers (DEBUG level only)
//                logResponse(logger, url, responseHttp);
//
//                // store the status code and message
//                int statusCode = responseHttp.code();
//                String statusMessage = responseHttp.message();
//
//                if (statusCode == 0) {
//                    throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
//                }
//
//                // Create a map of the status attributes that are always written to the request and response FlowFiles
//                Map<String, String> statusAttributes = new HashMap<>();
//                statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
//                statusAttributes.put(STATUS_MESSAGE, statusMessage);
//                statusAttributes.put(REQUEST_URL, url.toExternalForm());
//                statusAttributes.put(TRANSACTION_ID, txId.toString());
//
//                if (requestFlowFile != null) {
//                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
//                }
//
//                // If the property to add the response headers to the request flowfile is true then add them
////                if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean() && requestFlowFile != null) {
////                    // write the response headers as attributes
////                    // this will overwrite any existing flowfile attributes
////                    requestFlowFile = session.putAllAttributes(requestFlowFile, convertAttributesFromHeaders(url, responseHttp));
////                }
//
//                boolean outputBodyToRequestAttribute = !isSuccess(statusCode) && requestFlowFile != null;
//                boolean outputBodyToResponseContent = isSuccess(statusCode);
//                ResponseBody responseBody = responseHttp.body();
//                boolean bodyExists = responseBody != null;
//
//                InputStream responseBodyStream = null;
//                SoftLimitBoundedByteArrayOutputStream outputStreamToRequestAttribute = null;
//                TeeInputStream teeInputStream = null;
//                try {
//                    responseBodyStream = bodyExists ? responseBody.byteStream() : null;
//                    if (responseBodyStream != null && outputBodyToRequestAttribute && outputBodyToResponseContent) {
//                        outputStreamToRequestAttribute = new SoftLimitBoundedByteArrayOutputStream(256);
//                        teeInputStream = new TeeInputStream(responseBodyStream, outputStreamToRequestAttribute);
//                    }
//
//                    if (outputBodyToResponseContent) {
//                        /*
//                         * If successful and putting to response flowfile, store the response body as the flowfile payload
//                         * we include additional flowfile attributes including the response headers and the status codes.
//                         */
//
//                        // clone the flowfile to capture the response
//                        if (requestFlowFile != null) {
//                            responseFlowFile = session.create(requestFlowFile);
//                        } else {
//                            responseFlowFile = session.create();
//                        }
//
//                        // write attributes to response flowfile
//                        responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);
//
//                        // write the response headers as attributes
//                        // this will overwrite any existing flowfile attributes
//                        responseFlowFile = session.putAllAttributes(responseFlowFile, convertAttributesFromHeaders(url, responseHttp));
//
//                        // transfer the message body to the payload
//                        // can potentially be null in edge cases
//                        if (bodyExists) {
//                            // write content type attribute to response flowfile if it is available
//                            if (responseBody.contentType() != null) {
//                                responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), responseBody.contentType().toString());
//                            }
//                            if (teeInputStream != null) {
//                                responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
//                            } else {
//                                responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
//                            }
//
//                            // emit provenance event
//                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
//                            if (requestFlowFile != null) {
//                                session.getProvenanceReporter().fetch(responseFlowFile, url.toExternalForm(), millis);
//                            } else {
//                                session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);
//                            }
//                        }
//                    }
//
//                    // if not successful and request flowfile is not null, store the response body into a flowfile attribute
//                    if (outputBodyToRequestAttribute && bodyExists) {
//                        String attributeKey = RESPONSE_BODY;
//                        byte[] outputBuffer;
//                        int size;
//
//                        if (outputStreamToRequestAttribute != null) {
//                            outputBuffer = outputStreamToRequestAttribute.getBuffer();
//                            size = outputStreamToRequestAttribute.size();
//                        } else {
//                            outputBuffer = new byte[256];
//                            size = StreamUtils.fillBuffer(responseBodyStream, outputBuffer, false);
//                        }
//                        String bodyString = new String(outputBuffer, 0, size, getCharsetFromMediaType(responseBody.contentType()));
//                        requestFlowFile = session.putAttribute(requestFlowFile, attributeKey, bodyString);
//
//                        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
//                        session.getProvenanceReporter().modifyAttributes(requestFlowFile, "The " + attributeKey + " has been added. The value of which is the body of a http call to "
//                                + url.toExternalForm() + ". It took " + millis + "millis,");
//                    }
//                } finally {
//                    if (outputStreamToRequestAttribute != null) {
//                        outputStreamToRequestAttribute.close();
//                        outputStreamToRequestAttribute = null;
//                    }
//                    if (teeInputStream != null) {
//                        teeInputStream.close();
//                        teeInputStream = null;
//                    } else if (responseBodyStream != null) {
//                        responseBodyStream.close();
//                        responseBodyStream = null;
//                    }
//                }
//
//                route(requestFlowFile, session, context, statusCode);
//
//            }
//
//        } catch (final Exception e) {
//            // penalize or yield
//            if (requestFlowFile != null) {
//                logger.error("Routing to {} due to exception: {}", new Object[]{FAILURE.getName(), e}, e);
//                requestFlowFile = session.penalize(requestFlowFile);
//                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_CLASS, e.getClass().getName());
//                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_MESSAGE, e.getMessage());
//                // transfer original to failure
//                session.transfer(requestFlowFile, FAILURE);
//            } else {
//                logger.error("Yielding processor due to exception encountered as a source processor: {}", e);
//                context.yield();
//            }
//
//
//            // cleanup response flowfile, if applicable
//            try {
//                if (responseFlowFile != null) {
//                    session.remove(responseFlowFile);
//                }
//            } catch (final Exception e1) {
//                logger.error("Could not cleanup response flowfile due to exception: {}", new Object[]{e1}, e1);
//            }
//        }

        // TODO implement
    }

//    private void route(FlowFile request, ProcessSession session, ProcessContext context, int statusCode) {
//
//        if (!isSuccess(statusCode) && request == null) {
//            context.yield();
//        }
//        if (isSuccess(statusCode) && request != null) {
//            session.transfer(request, SUCCESS);
//
//        } else {
//            session.transfer(request, FAILURE);
//        }
//
//    }
//
//
//    private Map<String, String> convertAttributesFromHeaders(URL url, Response responseHttp) {
//        // create a new hashmap to store the values from the connection
//        Map<String, String> map = new HashMap<>();
//        responseHttp.headers().names().forEach((key) -> {
//            if (key == null) {
//                return;
//            }
//
//            List<String> values = responseHttp.headers().values(key);
//
//            // we ignore any headers with no actual values (rare)
//            if (values == null || values.isEmpty()) {
//                return;
//            }
//
//            // create a comma separated string from the values, this is stored in the map
//            String value = csv(values);
//
//            // put the csv into the map
//            map.put(key, value);
//        });
//
//        if (responseHttp.request().isHttps()) {
//            Principal principal = responseHttp.handshake().peerPrincipal();
//
//            if (principal != null) {
//                map.put(REMOTE_DN, principal.getName());
//            }
//        }
//
//        return map;
//    }
//
//    private Charset getCharsetFromMediaType(MediaType contentType) {
//        return contentType != null ? contentType.charset(StandardCharsets.UTF_8) : StandardCharsets.UTF_8;
//    }
//
//    private String csv(Collection<String> values) {
//        if (values == null || values.isEmpty()) {
//            return "";
//        }
//        if (values.size() == 1) {
//            return values.iterator().next();
//        }
//
//        StringBuilder sb = new StringBuilder();
//        for (String value : values) {
//            value = value.trim();
//            if (value.isEmpty()) {
//                continue;
//            }
//            if (sb.length() > 0) {
//                sb.append(", ");
//            }
//            sb.append(value);
//        }
//        return sb.toString().trim();
//    }
//
//    private boolean isSuccess(int statusCode) {
//        return statusCode / 100 == 2;
//    }
//
//    private Request configureRequest(URL url) {
//        Request.Builder requestBuilder = new Request.Builder().url(url).get();
//        return requestBuilder.build();
//    }
//
//    private void logRequest(ComponentLog logger, Request request) {
//        logger.debug("\nRequest to remote service:\n\t{}\n{}",
//                new Object[]{request.url().url().toExternalForm(), getLogString(request.headers().toMultimap())});
//    }
//
//    private void logResponse(ComponentLog logger, URL url, Response response) {
//        logger.debug("\nResponse from remote service:\n\t{}\n{}",
//                new Object[]{url.toExternalForm(), getLogString(response.headers().toMultimap())});
//    }
//
//    private String getLogString(Map<String, List<String>> map) {
//        StringBuilder sb = new StringBuilder();
//        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
//            List<String> list = entry.getValue();
//            if (list.isEmpty()) {
//                continue;
//            }
//            sb.append("\t");
//            sb.append(entry.getKey());
//            sb.append(": ");
//            if (list.size() == 1) {
//                sb.append(list.get(0));
//            } else {
//                sb.append(list.toString());
//            }
//            sb.append("\n");
//        }
//        return sb.toString();
//    }
}
