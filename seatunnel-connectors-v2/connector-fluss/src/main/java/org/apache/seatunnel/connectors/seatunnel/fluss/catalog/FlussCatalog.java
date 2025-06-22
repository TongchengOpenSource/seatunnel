package org.apache.seatunnel.connectors.seatunnel.fluss.catalog;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName FlussCatalog
 * @Description TODO
 * @Author joker
 * @Date 2025/6/21 17:57
 * @Version 1.0
 **/
public class FlussCatalog implements Catalog {
    private final String catalogName;
    private final ReadonlyConfig config;

    private Admin admin;

    public FlussCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.config = config;
    }

    @Override
    public void open() throws CatalogException {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        conf.setString("client.security.protocol", "sasl");
        conf.setString("client.security.sasl.mechanism", "PLAIN");
        conf.setString("client.security.sasl.username", "alice");
        conf.setString("client.security.sasl.password", "alice-secret");
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            this.admin = connection.getAdmin();
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return admin.listDatabases().get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        try {
            this.admin.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        List<String> databases = this.listDatabases();
        return databases.contains(databaseName);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return admin.listTables(databaseName).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {

        com.alibaba.fluss.metadata.TablePath flussTable = com.alibaba.fluss.metadata.TablePath.of(tablePath.getDatabaseName(), tablePath.getTableName());

        try {
            return admin.tableExists(flussTable).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
