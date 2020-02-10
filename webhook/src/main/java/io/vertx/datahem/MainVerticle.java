package io.vertx.datahem;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.datahem.pubsub.PubsubVerticle;
import java.util.logging.Logger;

public class MainVerticle extends AbstractVerticle {

    private final static Logger LOGGER = Logger.getLogger("MainVerticle");

    @Override
    public void start(Promise<Void> promise) throws Exception { 
        Promise<String> pubsubVerticleDeployment = Promise.promise();
        vertx.deployVerticle(new PubsubVerticle(), pubsubVerticleDeployment);

        pubsubVerticleDeployment.future().compose(id -> {
            Promise<String> httpVerticleDeployment = Promise.promise();
            vertx.deployVerticle(
                "io.vertx.datahem.http.HttpServerVerticle",
                new DeploymentOptions().setInstances(1),
            httpVerticleDeployment);
            return httpVerticleDeployment.future();
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        LOGGER.info("MainVerticle started: " + System.currentTimeMillis());
    }

    /*
    @Override
    public void start(Promise<Void> promise) throws Exception {  

        Promise<String> deployHttpPromise = Promise.promise();
        vertx.deployVerticle(
            "io.vertx.starter.http.HttpServerVerticle",
            new DeploymentOptions().setInstances(1),
            deployHttpPromise);
        Future<String> deployHttpFuture = deployHttpPromise.future();
        deployHttpFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
    }*/
}