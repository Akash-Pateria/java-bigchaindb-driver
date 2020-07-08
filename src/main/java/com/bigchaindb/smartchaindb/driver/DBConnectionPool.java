package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.stardog.stark.io.RDFFormats;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

public class DBConnectionPool {

    private static AdminConnection adminConn = null;
    private static ConnectionPool poolInstance = null;

    private static ConnectionPool creatConnectionPool() throws StardogException {
        try (AdminConnection connection = AdminConnectionConfiguration.toServer(StardogConstants.SERVER)
                .credentials(StardogConstants.ADMIN_USERNAME, StardogConstants.ADMIN_PASSWORD).connect()) {

            if (!connection.list().contains(StardogConstants.DB_NAME)) {
                // connection.drop(StardogConstants.DB_NAME);
                connection.disk(StardogConstants.DB_NAME).create();
                importOntology();
            }
            connection.close();

            ConnectionConfiguration connectionConfig = ConnectionConfiguration.to(StardogConstants.DB_NAME)
                    .server(StardogConstants.SERVER)
                    .credentials(StardogConstants.ADMIN_USERNAME, StardogConstants.ADMIN_PASSWORD);

            ConnectionPoolConfig PoolConfig = ConnectionPoolConfig.using(connectionConfig).minPool(10).maxPool(200)
                    .expiration(300, TimeUnit.SECONDS).blockAtCapacity(900, TimeUnit.SECONDS);
            poolInstance = PoolConfig.create();
        }
        return poolInstance;
    }

    public static void destroyConnectionPool() {
        if (poolInstance != null) {
            poolInstance.shutdown();
        }
    }

    private static void importOntology() {
        try (Connection connect = poolInstance.obtain()) {
            try {
                connect.begin();
                connect.add().io().format(RDFFormats.RDFXML)
                        .stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));
                connect.commit();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    poolInstance.release(connect);
                } catch (StardogException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static ConnectionPool getInstance() throws StardogException {
        // if (poolInstance == null)
        poolInstance = creatConnectionPool();
        return poolInstance;
    }

    public static void drop() {
        try {
            if (adminConn != null)
                adminConn.drop(StardogConstants.DB_NAME);
        } catch (StardogException e) {
            e.printStackTrace();
        }
    }
}
