package org.springframework.integration.avro.transformer;

import static org.springframework.integration.avro.io.AvroEncoderFactories.*;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.avro.AvroHeaders;
import org.springframework.integration.avro.generic.AvroGenericContainers;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * The Class AvroToBytesTransformer.
 */
public class AvroToBytesTransformer extends AbstractTransformer {

    @SuppressWarnings("unused")
    private static final Log LOGGER = LogFactory.getLog(AvroToBytesTransformer.class);

    private final EncoderFactory encoderFactory;

    /**
     * Instantiates a new AvroToBytesTransformer with the {@link #DEFAULT_ENCODER_FACTORY}
     */
    public AvroToBytesTransformer() {
        this(DEFAULT_ENCODER_FACTORY);
    }

    /**
     * Instantiates a new AvroToBytesTransformer with the supplied {@link EncoderFactory}.
     *
     * @param encoderFactory the encoder factory
     */
    public AvroToBytesTransformer(@NonNull EncoderFactory encoderFactory) {
        this.encoderFactory = encoderFactory;
    }

    @Override
    protected Object doTransform(@NonNull Message<?> message) {
        Object payload = message.getPayload();
        Assert.state(payload instanceof GenericContainer, "Payload must be an implementation of 'GenericContainer'");
        final GenericContainer payloadAsGenericContainer = (GenericContainer) payload;
        byte[] payloadAsBytes = AvroGenericContainers.toBytes(payloadAsGenericContainer, this.encoderFactory);
        return getMessageBuilderFactory().withPayload(payloadAsBytes)
            .copyHeaders(message.getHeaders())
            .copyHeadersIfAbsent(AvroHeaders.genericContainerToHeaders(payloadAsGenericContainer))
            .build();
    }
}
