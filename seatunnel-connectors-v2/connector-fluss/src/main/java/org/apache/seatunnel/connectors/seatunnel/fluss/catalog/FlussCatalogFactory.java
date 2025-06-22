package org.apache.seatunnel.connectors.seatunnel.fluss.catalog;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussBaseOptions;

/**
 * @ClassName FlussCatalogFactory
 * @Description TODO
 * @Author joker
 * @Date 2025/6/21 18:13
 * @Version 1.0
 **/
@AutoService(Factory.class)
public class FlussCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new FlussCatalog(catalogName, options);
    }

    @Override
    public String factoryIdentifier() {
        return FlussBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
