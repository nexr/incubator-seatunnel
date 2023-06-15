package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

public class ClickHouseTypeMapper implements JdbcDialectTypeMapper {

    /**
     * https://clickhouse.com/docs/en/sql-reference/data-types
     */
    private static final String CH_Bool = "BOOLEAN";
    private static final String CH_Date = "Date";
    private static final String CH_Date32 = "Date32";
    private static final String CH_DateTime = "DateTime";
    private static final String CH_DateTime32 = "DateTime32";
    private static final String CH_DateTime64 = "DateTime64";
    private static final String CH_Enum = "ENUM";
    private static final String CH_Enum8 = "ENUM8";
    private static final String CH_Enum16 = "ENUM16";
    private static final String CH_FixedString = "FixedString";
    private static final String CH_Int8 = "Int8";
    private static final String CH_UInt8 = "UInt8";
    private static final String CH_Int16 = "Int16";
    private static final String CH_UInt16 = "UInt16";
    private static final String CH_Int32 = "Int32";
    private static final String CH_UInt32 = "UInt32";
    private static final String CH_Int64 = "Int64";
    private static final String CH_IntervalYear = "IntervalYear";
    private static final String CH_IntervalQuarter = "IntervalQuarter";
    private static final String CH_IntervalMonth = "IntervalMonth";
    private static final String CH_IntervalWeek = "IntervalWeek";
    private static final String CH_IntervalDay = "IntervalDay";
    private static final String CH_IntervalHour = "IntervalHour";
    private static final String CH_IntervalMinute = "IntervalMinute";
    private static final String CH_IntervalSecond = "IntervalSecond";
    private static final String CH_IntervalMicrosecond = "IntervalMicrosecond";
    private static final String CH_IntervalMillisecond = "IntervalMillisecond";
    private static final String CH_IntervalNanosecond = "IntervalNanosecond";
    private static final String CH_UInt64 = "UInt64";
    private static final String CH_Int128 = "Int128";
    private static final String CH_UInt128 = "UInt128";
    private static final String CH_Int256 = "Int256";
    private static final String CH_UInt256 = "UInt256";
    private static final String CH_Decimal = "Decimal";
    private static final String CH_Decimal32 = "Decimal32";
    private static final String CH_Decimal64 = "Decimal64";
    private static final String CH_Decimal128 = "Decimal128";
    private static final String CH_Decimal256 = "Decimal256";
    private static final String CH_Float32 = "Float32";
    private static final String CH_Float64 = "Float64";
    private static final String CH_IPv4 = "IPv4";
    private static final String CH_IPv6 = "IPv6";
    private static final String CH_UUID = "UUID";
    private static final String CH_Point = "Point";
    private static final String CH_Polygon = "Polygon";
    private static final String CH_MultiPolygon = "MultiPolygon";
    private static final String CH_Ring = "Ring";
    private static final String CH_JSON = "JSON";
    private static final String CH_Object = "Object";
    private static final String CH_String = "String";
    private static final String CH_Array = "Array";
    private static final String CH_Map = "Map";
    private static final String CH_Nested = "Nested";
    private static final String CH_Tuple = "Tuple";
    private static final String CH_Nothing = "Nothing";
    private static final String CH_SimpleAggregateFunction = "SimpleAggregateFunction";
    private static final String CH_AggregateFunction = "AggregateFunction";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
        throws SQLException {

        String chType = unwrapCommonPrefix(metadata.getColumnTypeName(colIndex));
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (chType) {
            case CH_Bool:
                return BasicType.BOOLEAN_TYPE;
            case CH_Date:
            case CH_Date32:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case CH_DateTime:
            case CH_DateTime32:
            case CH_DateTime64:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case CH_Int8:
            case CH_UInt8:
            case CH_Int16:
            case CH_UInt16:
            case CH_Int32:
                return BasicType.INT_TYPE;
            case CH_UInt32:
            case CH_Int64:
            case CH_IntervalYear:
            case CH_IntervalQuarter:
            case CH_IntervalMonth:
            case CH_IntervalWeek:
            case CH_IntervalDay:
            case CH_IntervalHour:
            case CH_IntervalMinute:
            case CH_IntervalSecond:
            case CH_IntervalMicrosecond:
            case CH_IntervalMillisecond:
            case CH_IntervalNanosecond:
                return BasicType.LONG_TYPE;
            case CH_UInt64:
            case CH_Int128:
            case CH_UInt128:
            case CH_Int256:
            case CH_UInt256:
            case CH_Decimal:
            case CH_Decimal32:
            case CH_Decimal64:
            case CH_Decimal128:
            case CH_Decimal256:
            return new DecimalType(precision, scale);
            case CH_Float32:
                return BasicType.FLOAT_TYPE;
            case CH_Float64:
                return BasicType.DOUBLE_TYPE;
            case CH_Point:
            case CH_Ring:
            case CH_Polygon:
            case CH_MultiPolygon:
            case CH_Array:
            case CH_Map:
            case CH_Nested:
            case CH_Tuple:
                return PrimitiveByteArrayType.INSTANCE;
            case CH_Enum:
            case CH_Enum8:
            case CH_Enum16:
            case CH_IPv4:
            case CH_IPv6:
            case CH_JSON:
            case CH_Object:
            case CH_FixedString:
            case CH_String:
            case CH_UUID:
                return BasicType.STRING_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format(
                        "Doesn't support ClickHouse type '%s' on column '%s'  yet.",
                        chType, jdbcColumnName));
        }
    }

    private static final Pattern NULLABLE = Pattern.compile("Nullable\\((.*)\\)");
    private static final Pattern LOW_CARDINALITY = Pattern.compile("LowCardinality\\((.*)\\)");

    private String unwrapCommonPrefix(String fieldType) {
        Matcher nullMatcher = NULLABLE.matcher(fieldType);
        Matcher lowMatcher = LOW_CARDINALITY.matcher(fieldType);
        if (nullMatcher.matches()) {
            return nullMatcher.group(1);
        } else if (lowMatcher.matches()) {
            return lowMatcher.group(1);
        } else {
            return fieldType;
        }
    }

}
