package org.springframework.integration.avro.io;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * The Class AvroEncoderFactories.
 */
public final class AvroEncoderFactories {

    /**
     * The default thread safe immutable {@link EncoderFactory}, obtained via
     * {@link EncoderFactory#get}.
     */
    public static final EncoderFactory DEFAULT_ENCODER_FACTORY = EncoderFactory.get();

    /**
     * Factory method for creating immutable and thread safe {@link EncoderFactory} instances.
     *
     * @param binaryBufferSize the binary buffer size
     * @param binaryBlockSize the binary block size
     * @return the encoder factory
     */
    public static final EncoderFactory of(int binaryBufferSize, int binaryBlockSize) {
        return new DefaultImmutableEncoderFactory(binaryBufferSize, binaryBlockSize);
    }

    /**
     * Super type for immutable and thread safe {@link EncoderFactory} implementations.
     * <p>
     * Required as {@link EncoderFactory} does not expose its
     */
    abstract public static class AbstractImmutableEncoderFactory extends EncoderFactory {

        /**
         * Instantiates a new abstract immutable encoder factory.
         *
         * @param binaryBufferSize the binary buffer size
         * @param binaryBlockSize the binary block size
         */
        protected AbstractImmutableEncoderFactory(int binaryBufferSize, int binaryBlockSize) {
            this.binaryBufferSize = binaryBufferSize;
            this.binaryBlockSize = binaryBlockSize;

        }

        /**
         * Unsupported on immutable {@link EncoderFactory} implementations.
         * <p>
         * Guaranteed to throw an {@AvroRuntimeException} and leave the factory unmodified.
         * <p>
         * <b>NOTE</b> Avro's {@link DecoderFactory} and {@link EncoderFactory} immutable instances throw different
         * exceptions when attempting to call mutator methods.
         * This implementation adheres to that behavior.
         *
         * @param size the size
         * @return the encoder factory
         * @throws AvroRuntimeException always.
         */
        @Override
        public EncoderFactory configureBlockSize(int size) throws AvroRuntimeException {
            throw new AvroRuntimeException("Immutable encoder factories cannot be configured");
        }

        /**
         * Unsupported on immutable {@link EncoderFactory} implementations.
         * <p>
         * Guaranteed to throw an {@AvroRuntimeException} and leave the factory unmodified.
         * <p>
         * <b>NOTE</b> Avro's {@link DecoderFactory} and {@link EncoderFactory} immutable instances throw different
         * exceptions when attempting to call mutator methods.
         * This implementation adheres to that behavior.
         *
         * @param size the size
         * @return the encoder factory
         * @throws AvroRuntimeException Always thrown.
         */
        @Override
        public EncoderFactory configureBufferSize(int size) throws AvroRuntimeException {
            throw new AvroRuntimeException("Immutable encoder factories cannot be configured");
        }
    }

    /**
     * An immutable implementation of {@link EncoderFactory}
     */
    public static class DefaultImmutableEncoderFactory extends AbstractImmutableEncoderFactory {

        /**
         * Instantiates a new default immutable encoder factory.
         *
         * @param binaryBufferSize the binary buffer size
         * @param binaryBlockSize the binary block size
         */
        public DefaultImmutableEncoderFactory(int binaryBufferSize, int binaryBlockSize) {
            super(binaryBufferSize, binaryBlockSize);

        }

    }
}
