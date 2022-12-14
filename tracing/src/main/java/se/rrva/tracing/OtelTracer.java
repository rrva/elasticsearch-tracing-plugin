/*
 * Licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * Modified by Ragnar Rova on 2022-10-31
 *
 * Largely based on APMTracer.java, which is Copyright Elasticsearch B.V:
 *
 * https://github.com/elastic/elasticsearch/blob/f7bb5e02c50e2e981784fbf967c4531116be9dd4/modules/apm/src/main/java/org/elasticsearch/tracing/apm/APMTracer.java
 *
 */

package se.rrva.tracing;


import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporterBuilder;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tracing.Tracer;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

/**
 * A tracer that uses opentelemetry to collect spans. Largely based on
 */
public class OtelTracer extends AbstractLifecycleComponent implements Tracer {

  private static final Logger logger = LogManager.getLogger(OtelTracer.class);

  private final Map<String, Context> spans = ConcurrentCollections.newConcurrentMap();

  private io.opentelemetry.api.trace.Tracer tracer;
  private static OpenTelemetry openTelemetry;
  private String clusterName;
  private String nodeName;

  private static final String TRACE_CONTEXT = "apm.local.context";

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  protected void doStart() {
    String OTEL_EXPORTER_JAEGER_ENDPOINT = System.getenv("OTEL_EXPORTER_JAEGER_ENDPOINT");


    Resource resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "elasticsearch")));

    JaegerGrpcSpanExporterBuilder jaegerGrpcSpanExporterBuilder = JaegerGrpcSpanExporter.builder();
    if (OTEL_EXPORTER_JAEGER_ENDPOINT != null) {
      logger.info("Initializing otel tracing, jaeger_endpoint=" + OTEL_EXPORTER_JAEGER_ENDPOINT);

      jaegerGrpcSpanExporterBuilder.setEndpoint(OTEL_EXPORTER_JAEGER_ENDPOINT);
    } else {
      logger.info("Initializing otel tracing, jaeger_endpoint=http://localhost:14250. Set env OTEL_EXPORTER_JAEGER_ENDPOINT to override");

      jaegerGrpcSpanExporterBuilder.setEndpoint("http://localhost:14250");
    }
    JaegerGrpcSpanExporter jaegerGrpcSpanExporter = AccessController.doPrivileged((PrivilegedAction<JaegerGrpcSpanExporter>) jaegerGrpcSpanExporterBuilder::build);
    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(jaegerGrpcSpanExporter).build())
            .setResource(resource)
            .build();

    openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(B3Propagator.injectingMultiHeaders()))
            .buildAndRegisterGlobal();

    tracer = openTelemetry.getTracer("elasticsearch", Version.CURRENT.toString());

  }

  @Override
  protected void doStop() {

  }

  @Override
  protected void doClose() throws IOException {

  }

  @Override
  public void startTrace(ThreadContext threadContext, String id, String name, Map<String, Object> attributes) {
    spans.computeIfAbsent(id, _spanId -> AccessController.doPrivileged((PrivilegedAction<Context>) () -> {
      logger.trace("Tracing [{}] [{}]", id, name);
      final SpanBuilder spanBuilder = tracer.spanBuilder(name);

      // A span can have a parent span, which here is modelled though a parent span context.
      // Setting this is important for seeing a complete trace in the APM UI.
      final Context parentContext = getParentContext(threadContext);
      if (parentContext != null) {
        spanBuilder.setParent(parentContext);
      }

      setSpanAttributes(threadContext, attributes, spanBuilder);
      final Span span = spanBuilder.startSpan();
      final Context contextForNewSpan = Context.current().with(span);

      updateThreadContext(threadContext, contextForNewSpan);

      return contextForNewSpan;
    }));
  }

  private static void updateThreadContext(ThreadContext threadContext, Context context) {
    // The new span context can be used as the parent context directly within the same Java process...
    threadContext.putTransient(TRACE_CONTEXT, context);

    // ...whereas for tasks sent to other ES nodes, we need to put trace HTTP headers into the threadContext so
    // that they can be propagated.
    openTelemetry.getPropagators().getTextMapPropagator().inject(context, threadContext, (tc, key, value) -> {
      if (isSupportedContextKey(key)) {
        tc.putHeader(key, value);
      }
    });
  }

  private Context getParentContext(ThreadContext threadContext) {
    // https://github.com/open-telemetry/opentelemetry-java/discussions/2884#discussioncomment-381870
    // If you just want to propagate across threads within the same process, you don't need context propagators (extract/inject).
    // You can just pass the Context object directly to another thread (it is immutable and thus thread-safe).

    // Attempt to fetch a local parent context first, otherwise look for a remote parent
    Context parentContext = threadContext.getTransient("parent_" + TRACE_CONTEXT);
    if (parentContext == null) {
      final String traceParentHeader = threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER);
      final String traceStateHeader = threadContext.getTransient("parent_" + Task.TRACE_STATE);

      if (traceParentHeader != null) {
        final Map<String, String> traceContextMap = Maps.newMapWithExpectedSize(2);
        // traceparent and tracestate should match the keys used by W3CTraceContextPropagator
        traceContextMap.put(Task.TRACE_PARENT_HTTP_HEADER, traceParentHeader);
        if (traceStateHeader != null) {
          traceContextMap.put(Task.TRACE_STATE, traceStateHeader);
        }
        parentContext = openTelemetry.getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), traceContextMap, new MapKeyGetter());
      }
    }
    return parentContext;
  }

  /**
   * Most of the examples of how to use the OTel API look something like this, where the span context
   * is automatically propagated:
   *
   * <pre>{@code
   * Span span = tracer.spanBuilder("parent").startSpan();
   * try (Scope scope = parentSpan.makeCurrent()) {
   *     // ...do some stuff, possibly creating further spans
   * } finally {
   *     span.end();
   * }
   * }</pre>
   * This typically isn't useful in Elasticsearch, because a {@link Scope} can't be used across threads.
   * However, if a scope is active, then the APM agent can capture additional information, so this method
   * exists to make it possible to use scopes in the few situation where it makes sense.
   *
   * @param spanId the ID of a currently-open span for which to open a scope.
   * @return a method to close the scope when you are finished with it.
   */
  @Override
  public Releasable withScope(String spanId) {
    final Context context = spans.get(spanId);
    if (context != null) {
      var scope = context.makeCurrent();
      return scope::close;
    }
    return () -> {
    };
  }

  private void setSpanAttributes(@Nullable Map<String, Object> spanAttributes, SpanBuilder spanBuilder) {
    if (spanAttributes != null) {
      for (Map.Entry<String, Object> entry : spanAttributes.entrySet()) {
        final String key = entry.getKey();
        final Object value = entry.getValue();
        if (value instanceof String) {
          spanBuilder.setAttribute(key, (String) value);
        } else if (value instanceof Long) {
          spanBuilder.setAttribute(key, (Long) value);
        } else if (value instanceof Integer) {
          spanBuilder.setAttribute(key, (Integer) value);
        } else if (value instanceof Double) {
          spanBuilder.setAttribute(key, (Double) value);
        } else if (value instanceof Boolean) {
          spanBuilder.setAttribute(key, (Boolean) value);
        } else {
          throw new IllegalArgumentException(
                  "span attributes do not support value type of [" + value.getClass().getCanonicalName() + "]"
          );
        }
      }

      final boolean isHttpSpan = spanAttributes.keySet().stream().anyMatch(key -> key.startsWith("http."));
      spanBuilder.setSpanKind(isHttpSpan ? SpanKind.SERVER : SpanKind.INTERNAL);
    } else {
      spanBuilder.setSpanKind(SpanKind.INTERNAL);
    }

    spanBuilder.setAttribute(org.elasticsearch.tracing.Tracer.AttributeKeys.NODE_NAME, nodeName);
    spanBuilder.setAttribute(org.elasticsearch.tracing.Tracer.AttributeKeys.CLUSTER_NAME, clusterName);
  }

  private void setSpanAttributes(ThreadContext threadContext, @Nullable Map<String, Object> spanAttributes, SpanBuilder spanBuilder) {
    setSpanAttributes(spanAttributes, spanBuilder);

    final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
    if (xOpaqueId != null) {
      spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
    }
  }

  @Override
  public void addError(String spanId, Throwable throwable) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.recordException(throwable);
    }
  }

  @Override
  public void setAttribute(String spanId, String key, boolean value) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.setAttribute(key, value);
    }
  }

  @Override
  public void setAttribute(String spanId, String key, double value) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.setAttribute(key, value);
    }
  }

  @Override
  public void setAttribute(String spanId, String key, long value) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.setAttribute(key, value);
    }
  }

  @Override
  public void setAttribute(String spanId, String key, String value) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.setAttribute(key, value);
    }
  }

  @Override
  public void stopTrace(String id) {
    final var span = Span.fromContextOrNull(spans.remove(id));
    if (span != null) {
      logger.trace("Finishing trace [{}]", id);
      span.end();
    }
  }

  @Override
  public void addEvent(String spanId, String eventName) {
    final var span = Span.fromContextOrNull(spans.get(spanId));
    if (span != null) {
      span.addEvent(eventName);
    }
  }

  private static class MapKeyGetter implements TextMapGetter<Map<String, String>> {

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
      return carrier.keySet();
    }

    @Override
    public String get(Map<String, String> carrier, String key) {
      return carrier.get(key);
    }
  }

  private static boolean isSupportedContextKey(String key) {
    return Task.TRACE_PARENT_HTTP_HEADER.equals(key) || Task.TRACE_STATE.equals(key);
  }
}