package org.meshr.collector.vertx.http;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.config.ConfigRetriever;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


import org.meshr.collector.vertx.pubsub.PubsubService;

import com.google.common.collect.Multimap;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Enumeration;
import java.util.UUID;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerVerticle extends AbstractVerticle {

    private final static Logger LOG = LoggerFactory.getLogger("HttpServerVerticle.class");

    public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
    public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
    private PubsubService pubsubService;
    private final static byte[] trackingGif = { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, (byte) 0x80, 0x0, 0x0,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0,
            0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b };

    @Override
    public void start(Promise<Void> promise) throws Exception {

        String pubsubQueue = config().getString(CONFIG_PUBSUB_QUEUE, "pubsub.queue");
        pubsubService = PubsubService.createProxy(vertx, pubsubQueue);
        
        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);

        Router apiRouter = Router.router(vertx);
        apiRouter
            .route()
            //.handler(CorsHandler.create("*")
            .handler(CorsHandler.create(config().getString("ALLOWED_ORIGINS_REGEX","*"))
            .allowedHeaders(allowedHeaders)
            .allowedMethods(allowedMethods));

        apiRouter
            .get("/topic/:id")
            .produces("text/*")
            .produces("image/*")
            .handler(this::apiGet);

        apiRouter
            .post()
            .handler(BodyHandler.create());
        
        apiRouter
            .post("/cookie")
            .handler(this::apiCookie);

        apiRouter
            .post("/topic/:id")
            .handler(this::apiPost);

        vertx
            .createHttpServer()
            .requestHandler(apiRouter)
            .listen(config().getInteger("HTTP_PORT", 8080),
                ar -> {
                    if (ar.succeeded()) {
                        LOG.info("HTTP server running on port: " + ar.result().actualPort());
                        promise.complete();
                    } else {
                        LOG.error("Could not start a HTTP server: " + ar.cause().toString());
                        promise.fail(ar.cause());
                    }
                }
            );
    }

    private void apiCookie(RoutingContext context) {
        
        Map<String,String> headers = getHeadersAsMap(context.request().headers());
        JsonArray cookies = context.getBodyAsJsonArray();
        Map<String, Cookie> cookieMap = context.cookieMap();
        cookies.forEach(object -> {
            JsonObject cookie = (JsonObject) object;
            if(cookie.containsKey("name") && cookie.containsKey("value")){
                LOG.info("cookie config: " + cookie.toString() + ", real cookie: " + cookieMap.get(cookie.getString("name")).encode());
                context.addCookie(
                    cookieMap
                        .get(cookie.getString("name"))
                        .setMaxAge(cookie.getLong("maxAge"))
                        .setDomain(cookie.getString("domain"))
                );
            }
        });
        context.response().setStatusCode(204).end();
    }

    private void apiPost(RoutingContext context) {
        
        Map<String,String> headers = getHeadersAsMap(context.request().headers());
        String payload = context.getBodyAsString();
        String topic = String.valueOf(context.request().getParam("id"));
        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(204).end();
            } else {
                LOG.error("apiPost handler reply fail: ", reply.cause());
                context.fail(reply.cause());
            }
        };
        pubsubService.publishMessage(payload, headers, topic, handler);
    }

    private void apiGet(RoutingContext context) {
        Map<String,String> headers = getHeadersAsMap(context.request().headers());
        String payload = context.request().query();
        String topic = String.valueOf(context.request().getParam("id"));
        
        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                if(context.getAcceptableContentType().equals("image/*")){
                    context.response()
                    .putHeader("content-type", "image/gif")
                    .setChunked(true)
                    .write(Buffer.buffer(trackingGif))
                    .putHeader("content-length", "trackingGif.length")
                    .setStatusCode(200)
                    .end();
                }else{
                    context.response().setStatusCode(204).end();
                }
            } else {
                LOG.info("apiGet handler reply fail: ", reply.cause());
                context.fail(reply.cause());
            }
        };
        pubsubService.publishMessage(payload, headers, topic, handler);
    }

    private Map<String,String> getHeadersAsMap(MultiMap headersMultiMap){
        return headersMultiMap
            .names()
			.stream()
			.map(s -> new String[]{s, headersMultiMap.get(s)})
            .collect(HashMap::new, (m,v)->m.put(v[0].toLowerCase(), v[1]), HashMap::putAll);
    }
}