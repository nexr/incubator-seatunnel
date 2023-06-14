package org.apache.seatunnel.transform.uuid;

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;

@Getter
@Setter
public class UUIDTransformConfig implements Serializable {

    public static final Option<String> UUID_FIELD =
            Options.key("uuid_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("This is the field for UUID");

    private String uuidField;

    public static UUIDTransformConfig of(ReadonlyConfig config) {
        UUIDTransformConfig uuidTransformConfig = new UUIDTransformConfig();
        uuidTransformConfig.setUuidField(config.get(UUID_FIELD));
        return uuidTransformConfig;
    }

}
