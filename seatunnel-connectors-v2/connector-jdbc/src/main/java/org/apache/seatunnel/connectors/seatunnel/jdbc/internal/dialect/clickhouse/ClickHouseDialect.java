package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

public class ClickHouseDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new ClickHouseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new ClickHouseTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
        String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // TODO(halo) : need to upsert statement
        return Optional.empty();
    }

    @Override
    public PreparedStatement creatPreparedStatement(
        Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(queryTemplate);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    @Override
    public ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSourceConfig jdbcSourceConfig) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(jdbcSourceConfig.getQuery());
        return ps.executeQuery().getMetaData();
    }
}
