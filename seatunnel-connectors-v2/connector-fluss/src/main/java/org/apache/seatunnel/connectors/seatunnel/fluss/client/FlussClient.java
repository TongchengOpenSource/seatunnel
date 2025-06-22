package org.apache.seatunnel.connectors.seatunnel.fluss.client;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import java.util.List;

/**
 * @ClassName FlussClient
 * @Description TODO
 * @Author joker
 * @Date 2025/6/21 23:09
 * @Version 1.0
 **/
public class FlussClient {

    private final SourceConfig sourceConfig;
    private final Connection connection;

    public FlussClient(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", sourceConfig.getBootstrapServers().get(0));
        conf.setString("client.security.protocol", "sasl");
        conf.setString("client.security.sasl.mechanism", "PLAIN");
        conf.setString("client.security.sasl.username", "alice");
        conf.setString("client.security.sasl.password", "alice-secret");
        connection = ConnectionFactory.createConnection(conf);
    }

    public List<TablePartition> findPartitions(String tableName) {

        TablePath tablePath = TablePath.of(sourceConfig.getDatabase(), tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();

        table.newScan().createLogScanner()


    }
}
