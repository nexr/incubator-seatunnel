package org.apache.seatunnel.transform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UUIDTransformFactoryTest {

    @Test
    public void testOptionRule() throws Exception {
        UUIDTransformFactory uuidTransformFactory = new UUIDTransformFactory();
        Assertions.assertNotNull(uuidTransformFactory.optionRule());
    }
}
