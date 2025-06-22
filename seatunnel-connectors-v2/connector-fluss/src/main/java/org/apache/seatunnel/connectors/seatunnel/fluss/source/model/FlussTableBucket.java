package org.apache.seatunnel.connectors.seatunnel.fluss.source.model;

import com.alibaba.fluss.metadata.TableBucket;
import lombok.Getter;

import java.io.Serializable;

/**
 * @ClassName FlussTableBucket
 * @Description TODO
 * @Author joker
 * @Date 2025/6/21 22:35
 * @Version 1.0
 **/
@Getter
public class FlussTableBucket implements Serializable, Comparable<FlussTableBucket> {

    private String database;
    private String table;
    private TableBucket tableBucket;

    @Override
    public int compareTo(FlussTableBucket o) {
        return 0;
    }
}
