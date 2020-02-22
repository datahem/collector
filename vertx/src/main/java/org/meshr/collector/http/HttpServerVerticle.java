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

//import java.util.logging.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="https://robertsahlin.com/">Robert Sahlin</a>
 */
public class HttpServerVerticle extends AbstractVerticle {

    private final static Logger LOGGER = LoggerFactory.getLogger("HttpServerVerticle.class");

    public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
    public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
    public static final String BACKUP_TOPIC = "tmp";
    private PubsubService pubsubService;
    //private final static Logger LOGGER = Logger.getLogger("HttpServerVerticle");
    private final static byte[] trackingGif = { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, (byte) 0x80, 0x0, 0x0,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0,
            0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b };
    private final static Handler<AsyncResult<Void>> latencyHandler = reply -> {
                    if (reply.succeeded()) {
                        LOGGER.info("publishMessage succeeded");
                    } else {
                        LOGGER.info("publishMessage fail: " + reply.cause().toString());
                    }
                };

    @Override
    public void start(Promise<Void> promise) throws Exception {
        //LOGGER.info("Creating vertx HTTP server: " + System.currentTimeMillis());

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
            .handler(CorsHandler.create("*")
            .allowedHeaders(allowedHeaders)
            .allowedMethods(allowedMethods));
        apiRouter
            .route("/optimize/:mode/topic/:id")
            .method(HttpMethod.GET)
            .produces("text/*")
            .produces("image/*")
            .handler(this::apiGet);
        apiRouter
            .post()
            .handler(BodyHandler.create());
        apiRouter
            .post("/optimize/:mode/topic/:id")
            .handler(this::apiPost);

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(
            config -> {
                if (config.failed()) {
                    LOGGER.error("Config failed: " + config.cause().toString());
                    promise.fail(config.cause());
                } else {
                    vertx
                        .createHttpServer()
                        .requestHandler(apiRouter)
                        .listen(config.result().getInteger("HTTP_PORT", 8080),
                            ar -> {
                                if (ar.succeeded()) {
                                    LOGGER.info("HTTP server running on port: " + ar.result().actualPort());
                                    promise.complete();
                                } else {
                                    LOGGER.error("Could not start a HTTP server: " + ar.cause().toString());
                                    promise.fail(ar.cause());
                                }
                            }
                        );
                }
            }
        );
    }

    private void apiPost(RoutingContext context) {
        
        Map<String,String> headers = getHeadersAsMap(context.request().headers());
        String payload = "hello"; //context.getBodyAsString();
        String topic = String.valueOf(context.request().getParam("id"));
        String mode = String.valueOf(context.request().getParam("mode"));
        LOGGER.info("Payload: " + payload);
        switch(mode){
            case "latencyd":
                pubsubService.publishMessage(payload, headers, topic, latencyHandler);
                context.response().setStatusCode(204).end();
                break;
            default:
                Handler<AsyncResult<Void>> handler = reply -> {
                    if (reply.succeeded()) {
                        context.response().setStatusCode(204).end();
                    } else {
                        //LOGGER.info("apiPost handler reply fail: " + reply.cause().toString());
                        context.fail(reply.cause());
                    }
                };
                pubsubService.publishMessage(payload, headers, topic, handler);
        }
        
    }

    private void apiGet(RoutingContext context) {
        //LOGGER.info(context.getAcceptableContentType());
        Map<String,String> headers = getHeadersAsMap(context.request().headers()); //"hello";//context.request().headers().asMap().toString();
        String payload = context.request().query();
        String topic = String.valueOf(context.request().getParam("id"));
        String mode = String.valueOf(context.request().getParam("mode")); 
        switch(mode){
            case "latency":
                pubsubService.publishMessage(payload, headers, topic, latencyHandler);
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
                break;
            default:
                Handler<AsyncResult<Void>> handler = reply -> {
                    if (reply.succeeded()) {
                        //LOGGER.info("apiGet handler succeeded: " + System.currentTimeMillis());
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
                        //LOGGER.info("apiGet handler reply fail: " + reply.cause().toString());
                        context.fail(reply.cause());
                    }
                };
                //LOGGER.info("apiGet publish message: " + System.currentTimeMillis());
                pubsubService.publishMessage(payload, headers, topic, handler);
        }
    }

    private Map<String,String> getHeadersAsMap(MultiMap headersMultiMap){
        return headersMultiMap
            .names()
			.stream()
			.map(s -> new String[]{s, headersMultiMap.get(s)})
            .collect(HashMap::new, (m,v)->m.put(v[0].toLowerCase(), v[1]), HashMap::putAll);
    }
    
}