package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

public class ClickHouseJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
