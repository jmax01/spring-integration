package org.springframework.integration.avro.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.integration.avro.AvroSchemas.SchemaPair;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.Assert;

/**
 * The Class AvroDatumReaders.
 */
public final class AvroDatumReaders {

    static final Map<Schema, Map<Schema, DatumReader<?>>> DATUM_READERS_BY_READER_SCHEMA_AND_WRITER_SCHEMA = new ConcurrentHashMap<>();

    static final Map<Class<?>, Map<SchemaPair, DatumReader<?>>> DATUM_READERS_BY_SCHEMA_PAIR_BY_CLASS = new ConcurrentHashMap<>();

    /**
     * To datum reader.
     *
     * @param <T> the generic type
     * @param writerSchema the writer schema
     * @param readerSchema the reader schema
     * @param genericData the generic data
     * @return the datum reader
     */
    @SuppressWarnings("unchecked")
    @NonNull
    public static <T extends GenericContainer> DatumReader<T> toDatumReader(@Nullable Schema writerSchema,
            @Nullable Schema readerSchema, @Nonnull GenericData genericData) {

        Assert.isTrue(readerSchema != null || writerSchema != null, "At least one schema must not be null");

        Schema writerKey = writerSchema == null ? readerSchema : writerSchema;
        Schema readerKey = readerSchema == null ? writerSchema : readerSchema;

        return (DatumReader<T>) DATUM_READERS_BY_READER_SCHEMA_AND_WRITER_SCHEMA
            .compute(writerKey,
                    (_writerKey, datumReadersByReaderSchema) -> datumReadersByReaderSchema == null
                            ? new ConcurrentHashMap<>()
                            : datumReadersByReaderSchema)
            .computeIfAbsent(readerKey, _readerKey -> {

                if (genericData instanceof ReflectData) {
                    return new ReflectDatumReader<>(writerKey, readerKey, (ReflectData) genericData);
                }

                if (genericData instanceof SpecificData) {
                    return new SpecificDatumReader<>(writerKey, readerKey, (SpecificData) genericData);
                }

                return new GenericDatumReader<>(writerKey, readerKey, genericData);
            });
    }

    /**
     * To datum reader.
     *
     * @param <T> the generic type
     * @param genericContainerClass the generic container class
     * @param writerSchema the writer schema
     * @param readerSchema the reader schema
     * @return the datum reader
     */
    public static <T extends GenericContainer> DatumReader<T> toDatumReader(@NonNull Class<T> genericContainerClass,
            @Nullable Schema writerSchema, @Nullable Schema readerSchema) {

        // SpecificRecord and SpecificFixed have their schemas available statically.
        if (SpecificRecord.class.isAssignableFrom(genericContainerClass)
                || SpecificFixed.class.isAssignableFrom(genericContainerClass)) {

            Schema actualReaderSchema = readerSchema == null ? SpecificData.get()
                .getSchema(genericContainerClass) : readerSchema;

            return toDatumReader(writerSchema, actualReaderSchema, SpecificData.get());

        } else if (GenericRecord.class.isAssignableFrom(genericContainerClass)
                || GenericFixed.class.isAssignableFrom(genericContainerClass)) {

            if (writerSchema != null) {

                return toDatumReader(writerSchema, readerSchema, GenericData.get());
            }

            throw new MessageConversionException("No schema can be inferred from type "
                    + genericContainerClass.getName() + " and no schema has been explicitly configured.");

        } else {

            Schema actualReaderSchema = readerSchema == null ? SpecificData.get()
                .getSchema(genericContainerClass) : readerSchema;

            return toDatumReader(writerSchema, actualReaderSchema, ReflectData.get());
        }
    }

}
