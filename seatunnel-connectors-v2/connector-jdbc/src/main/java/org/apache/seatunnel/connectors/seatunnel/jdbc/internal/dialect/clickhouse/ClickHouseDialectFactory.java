package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

@AutoService(JdbcDialectFactory.class)
public class ClickHouseDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:ch:");
    }

    @Override
    public JdbcDialect create() {
        return new ClickHouseDialect();
    }
}
