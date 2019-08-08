package org.springframework.integration.avro.io;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * The Class AvroDecoderFactories.
 */
public final class AvroDecoderFactories {

    /**
     * The default thread safe immutable {@link DecoderFactory}, obtained via
     * {@link DecoderFactory#get}.
     */
    public static final DecoderFactory DEFAULT_DECODER_FACTORY = DecoderFactory.get();

    /**
     * Factory method for creating immutable and thread safe {@link DecoderFactory} instances.
     *
     * @param binaryDecoderBufferSize the binary decoder buffer size
     * @return the decoder factory
     */
    public static final DecoderFactory of(int binaryDecoderBufferSize) {
        return new DefaultImmutableDecoderFactory(binaryDecoderBufferSize);
    }

    /**
     * Super type for immutable and thread safe {@link DecoderFactory} implementations.
     * <p>
     * Required as {@link DecoderFactory} does not expose its
     */
    abstract public static class AbstractImmutableDecoderFactory extends DecoderFactory {

        /**
         * Instantiates a new abstract immutable decoder factory.
         *
         * @param binaryDecoderBufferSize the binary decoder buffer size
         */
        protected AbstractImmutableDecoderFactory(int binaryDecoderBufferSize) {
            super.configureDecoderBufferSize(binaryDecoderBufferSize);
        }

        /**
         * Unsupported on immutable {@link DecoderFactory} implementations.
         * <p>
         * Guaranteed to throw an {@IllegalArgumentException} and leave the factory unmodified.
         * <p>
         * <b>NOTE</b> Avro's {@link DecoderFactory} and {@link EncoderFactory} immutable instances throw different
         * exceptions when attempting to call mutator methods.
         * This implementation adheres to that behavior.
         *
         * @param binaryDecoderBufferSize the binary decoder buffer size
         * @return the decoder factory
         * @throws IllegalArgumentException Always thrown.
         */
        @Override
        public DecoderFactory configureDecoderBufferSize(int binaryDecoderBufferSize) {
            throw new IllegalArgumentException("This Factory instance is Immutable");
        }
    }

    /**
     * An immutable implementation of {@link DecoderFactory}
     */
    public static class DefaultImmutableDecoderFactory extends AbstractImmutableDecoderFactory {

        /**
         * Instantiates a new default immutable decoder factory.
         *
         * @param binaryDecoderBufferSize the binary decoder buffer size
         */
        public DefaultImmutableDecoderFactory(int binaryDecoderBufferSize) {
            super(binaryDecoderBufferSize);

        }

    }
}
