package org.springframework.integration.avro.generic;

import static org.springframework.integration.avro.io.AvroDatumWriters.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.avro.AvroSchemas.SchemaPair;
import org.springframework.integration.avro.io.AvroDatumReaders;
import org.springframework.integration.avro.specific.SpecificFixedSupport;
import org.springframework.integration.avro.specific.SpecificRecords;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.FastByteArrayOutputStream;

/**
 * The Class AvroGenericContainers.
 */
public final class AvroGenericContainers {

    private static final Log LOGGER = LogFactory.getLog(AvroGenericContainers.class);

    /**
     * To schema pair.
     *
     * @param <T> the generic type
     * @param genericContainerType the generic container type
     * @return the optional
     */
    public static <
        T extends GenericContainer> Optional<SchemaPair> toSchemaPair(@NonNull Class<T> genericContainerType) {

        if (SpecificRecords.class.isAssignableFrom(genericContainerType)) {

            @SuppressWarnings("unchecked")
            final Schema classSchema = SpecificRecords.toClassSchema((Class<SpecificRecord>) genericContainerType);
            return Optional.of(SchemaPair.writerOnly(classSchema));
        }

        if (SpecificFixed.class.isAssignableFrom(genericContainerType)) {

            @SuppressWarnings("unchecked")
            final Schema classSchema = SpecificFixedSupport.toClassSchema((Class<SpecificFixed>) genericContainerType);
            return Optional.of(SchemaPair.writerOnly(classSchema));
        }

        return Optional.of(SchemaPair.writerOnly(ReflectData.get()
            .getSchema(genericContainerType)));

    }

    /**
     * To bytes.
     *
     * @param genericContainer the generic container
     * @param encoderFactory the encoder factory
     * @return the byte[]
     */
    @NonNull
    public static byte[] toBytes(@NonNull GenericContainer genericContainer, @NonNull EncoderFactory encoderFactory) {

        return toBytes(genericContainer, encoderFactory, null);
    }

    /**
     * To bytes.
     *
     * @param genericContainer the generic container
     * @param encoderFactory the encoder factory
     * @param binaryEncoderToReuse the {@link BinaryEncoder} to reuse or null
     * @return the byte[]
     */
    @NonNull
    public static byte[] toBytes(@NonNull GenericContainer genericContainer, @NonNull EncoderFactory encoderFactory,
            @Nullable BinaryEncoder binaryEncoderToReuse) {

        try (FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream()) {
            BinaryEncoder binaryEncoder = encoderFactory.directBinaryEncoder(fbaos, binaryEncoderToReuse);
            DatumWriter<Object> writer = toDatumWriter(genericContainer);
            writer.write(genericContainer, binaryEncoder);
            binaryEncoder.flush();
            return fbaos.toByteArray();
        } catch (IOException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Attempt to transform to byte[] failed for genericContainer: " + genericContainer, e);
            }
            throw new UncheckedIOException(e);
        }
    }

    /**
     * From byte array to the supplied {@link GenericContainer} type.
     *
     * @param <T> the target {@link GenericContainer} type
     * @param avroGenericContainerAsBytes the Avro {@link GenericContainer} as bytes
     * @param genericContainerClassSupplier a supplier of the target {@link GenericContainer} class
     * @param writerSchema the writer {@link Schema} or null
     * @param readerSchema the reader {@link Schema} or null
     * @param decoderFactory the {@link DecoderFactory}
     * @return the byte[]
     */
    @NonNull
    public static <T extends GenericContainer> T fromBytes(@NonNull byte[] avroGenericContainerAsBytes,
            @NonNull Supplier<? extends Class<T>> genericContainerClassSupplier, @Nullable Schema writerSchema,
            @Nullable Schema readerSchema, @NonNull DecoderFactory decoderFactory) {

        return fromBytes(avroGenericContainerAsBytes, genericContainerClassSupplier.get(), writerSchema, readerSchema,
                decoderFactory, null, null);
    }

    /**
     * From byte array to the supplied {@link GenericContainer} type.
     *
     * @param <T> the target {@link GenericContainer} type
     * @param avroGenericContainerAsBytes the Avro {@link GenericContainer} as bytes
     * @param genericContainerClass the {@link GenericContainer} class
     * @param writerSchema the writer {@link Schema} or null
     * @param readerSchema the reader {@link Schema} or null
     * @param decoderFactory the {@link DecoderFactory}
     * @return the byte[]
     */
    @NonNull
    public static <T extends GenericContainer> T fromBytes(@NonNull byte[] avroGenericContainerAsBytes,
            @NonNull Class<T> genericContainerClass, @Nullable Schema writerSchema, @Nullable Schema readerSchema,
            @NonNull DecoderFactory decoderFactory) {

        return fromBytes(avroGenericContainerAsBytes, genericContainerClass, writerSchema, readerSchema, decoderFactory,
                null, null);
    }

    /**
     * From byte array to the supplied {@link GenericContainer} type.
     *
     * @param <T> the target {@link GenericContainer} type
     * @param avroGenericContainerAsBytes the Avro {@link GenericContainer} as bytes
     * @param genericContainerClass the {@link GenericContainer} class
     * @param writerSchema the writer {@link Schema} or null
     * @param readerSchema the reader {@link Schema} or null
     * @param decoderFactory the {@link DecoderFactory}
     * @param binaryDecoderToReuse the {@link BinaryDecoder} to reuse or null
     * @param instanceToReuse the instance to reuse
     * @return the byte[]
     */
    @NonNull
    public static <T extends GenericContainer> T fromBytes(@NonNull byte[] avroGenericContainerAsBytes,
            @NonNull Class<T> genericContainerClass, @Nullable Schema writerSchema, @Nullable Schema readerSchema,
            @NonNull DecoderFactory decoderFactory, @Nullable BinaryDecoder binaryDecoderToReuse,
            @Nullable T instanceToReuse) {

        try {

            final BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(avroGenericContainerAsBytes,
                    binaryDecoderToReuse);

            final DatumReader<T> datumReader = AvroDatumReaders.toDatumReader(genericContainerClass, writerSchema,
                    readerSchema);

            return datumReader.read(instanceToReuse, binaryDecoder);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * From byte array to the supplied {@link GenericContainer} type.
     *
     * @param <T> the target {@link GenericContainer} type
     * @param avroGenericContainerAsBytes the Avro {@link GenericContainer} as bytes
     * @param genericContainerClassSupplier a supplier of the target {@link GenericContainer} class
     * @param writerSchema the writer {@link Schema} or null
     * @param readerSchema the reader {@link Schema} or null
     * @param decoderFactory the {@link DecoderFactory}
     * @param binaryDecoderToReuse the {@link BinaryDecoder} to reuse or null
     * @param instanceToReuse the instance to reuse or null
     * @return the byte[]
     */
    @NonNull
    public static <T extends GenericContainer> T fromBytes(@NonNull byte[] avroGenericContainerAsBytes,
            @NonNull Supplier<? extends Class<T>> genericContainerClassSupplier, @Nullable Schema writerSchema,
            @Nullable Schema readerSchema, @NonNull DecoderFactory decoderFactory,
            @Nullable BinaryDecoder binaryDecoderToReuse, @Nullable T instanceToReuse) {

        return fromBytes(avroGenericContainerAsBytes, genericContainerClassSupplier.get(), writerSchema, readerSchema,
                decoderFactory, binaryDecoderToReuse, instanceToReuse);
    }

}
