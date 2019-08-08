package org.springframework.integration.avro.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.lang.NonNull;

/**
 * The Class AvroDatumWriters.
 */
public final class AvroDatumWriters {

    /** The Constant DatumWriters by Schema. */
    protected static final Map<Schema, DatumWriter<Object>> DATUM_WRITER_CACHE = new ConcurrentHashMap<>();

    /**
     * To datum writer.
     *
     * @param genericContainer the generic container
     * @return the datum writer
     */
    public static DatumWriter<Object> toDatumWriter(@NonNull GenericContainer genericContainer) {

        return DATUM_WRITER_CACHE.computeIfAbsent(genericContainer.getSchema(),
                schema -> toDatumWriter(schema, genericContainer.getClass()));
    }

    /**
     * Depending on the type of the {@link GenericContainer} class, instantiates a new {@link SpecificDatumWriter}
     * or {@link GenericDatumWriter} with the supplied {@link Schema} and the default {@link SpecificData#get()} or
     * {@link GenericData#get()}.
     *
     * @param schema the {@link Schema} with which to write.
     * @param genericContainerClass The {@link GenericContainer} type to write.
     * @return the datum writer
     */
    static DatumWriter<Object> toDatumWriter(@NonNull Schema schema,
            @NonNull Class<? extends GenericContainer> genericContainerClass) {

        return toDatumWriter(schema, genericContainerClass, SpecificData.get(), GenericData.get());
    }

    /**
     * Depending on the type of the {@link GenericContainer} class, instantiates a new {@link SpecificDatumWriter}
     * or {@link GenericDatumWriter} with the supplied {@link Schema} and the default {@link SpecificData#get()} or
     * {@link GenericData#get()}.
     *
     * @param schema the {@link Schema} with which to write.
     * @param genericContainerClass The {@link GenericContainer} type to write.
     * @param specificData the {@link SpecificData} to use in if {@code genericContainer} is and instance of
     *            {@code genericContainer}
     * @return the datum writer
     */
    static DatumWriter<Object> toDatumWriter(@NonNull Schema schema,
            @NonNull Class<? extends GenericContainer> genericContainerClass, @NonNull SpecificData specificData,
            @NonNull GenericData genericData) {

        if (genericContainerClass.isAssignableFrom(SpecificRecord.class)) {

            return new SpecificDatumWriter<>(schema, specificData);

        }

        return new GenericDatumWriter<>(schema, genericData);
    }
}
