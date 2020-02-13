package io.vertx.datahem;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

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
}