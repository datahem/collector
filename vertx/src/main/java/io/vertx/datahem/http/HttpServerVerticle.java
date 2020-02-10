
package io.vertx.datahem.http;

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
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;


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

import io.vertx.datahem.pubsub.PubsubService;

import com.google.common.collect.Multimap;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Enumeration;
import java.util.UUID;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.logging.Logger;

/**
 * @author <a href="https://robertsahlin.com/">Robert Sahlin</a>
 */
public class HttpServerVerticle extends AbstractVerticle {

    public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
    public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
    public static final String BACKUP_TOPIC = "tmp";
    private PubsubService pubsubService;
    private final static Logger LOGGER = Logger.getLogger("HttpServerVerticle");
    private final static byte[] trackingGif = { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, (byte) 0x80, 0x0, 0x0,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0,
            0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b };

    @Override
    public void start(Promise<Void> promise) throws Exception {
        LOGGER.info("Creating vertx HTTP server: " + System.currentTimeMillis());

        String pubsubQueue = config().getString(CONFIG_PUBSUB_QUEUE, "pubsub.queue");
        pubsubService = PubsubService.createProxy(vertx, pubsubQueue);
        HttpServer server = vertx.createHttpServer();

        Router apiRouter = Router.router(vertx);
        apiRouter.route("/topic/:id").method(HttpMethod.GET).produces("text/*").produces("image/*").handler(this::apiGet);
        //apiRouter.get("/topic/:id").handler(this::apiGet);
        apiRouter.post().handler(BodyHandler.create());
        apiRouter.post("/topic/:id").handler(this::apiPost);

        int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        server
            .requestHandler(apiRouter)
            .listen(portNumber, ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("HTTP server running on port " + portNumber);
                    promise.complete();
                } else {
                    LOGGER.info("Could not start a HTTP server: " + ar.cause().toString());
                    promise.fail(ar.cause());
                }
            });
    }

    private void apiPost(RoutingContext context) {
        
        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(204);
                context.response().end();
            } else {
                LOGGER.info("apiPost handler reply fail: " + reply.cause().toString());
                context.fail(reply.cause());
            }
        };

        String headers = "hello";//context.request().headers().asMap().toString();
        
        String payload = context.getBodyAsString();//context.getBodyAsJson().toString();
        String topic = String.valueOf(context.request().getParam("id"));
        LOGGER.info("apiGet publish message: " + System.currentTimeMillis());
        pubsubService.publishMessage(payload, headers, topic, handler);
    }

    private void apiGet(RoutingContext context) {
        LOGGER.info(context.getAcceptableContentType());
        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                if(context.getAcceptableContentType().equals("image/*")){
                    context.response().putHeader("content-type", "image/gif");
                    context.response().putHeader("content-length", "trackingGif.length");
                    context.response().setStatusCode(200);
                    context.response().end(Buffer.buffer(trackingGif));
                }else{
                    context.response().setStatusCode(204);
                    context.response().end();
                }
            } else {
                LOGGER.info("apiGet handler reply fail: " + reply.cause().toString());
                context.fail(reply.cause());
            }
        };

        String headers = "hello";//context.request().headers().asMap().toString();
        String payload = context.request().query();
        String topic = String.valueOf(context.request().getParam("id"));
        LOGGER.info("apiGet publish message: " + System.currentTimeMillis());
        pubsubService.publishMessage(payload, headers, topic, handler);
  }
}