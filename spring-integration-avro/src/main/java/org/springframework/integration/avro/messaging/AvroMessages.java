package org.springframework.integration.avro.messaging;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.springframework.integration.avro.AvroSchemas.SchemaPair;
import org.springframework.integration.avro.io.AvroDatumReaders;
import org.springframework.lang.NonNull;

/**
 * The Class AvroMessages.
 */
public final class AvroMessages {

    /**
     * To datum reader.
     *
     * @param <T> the generic type
     * @param genericContainerType the generic container type
     * @param messageHeaders the message headers
     * @param toSchemaPair the to schema pair
     * @return the datum reader
     */
    public static final <T extends GenericContainer> DatumReader<T> toDatumReader(
            @NonNull Class<T> genericContainerType, Map<String, Object> messageHeaders,
            @NonNull BiFunction<? super Class<T>, ? super Map<String, Object>, ? extends Optional<SchemaPair>> toSchemaPair) {

        final Optional<SchemaPair> apply = toSchemaPair.apply(genericContainerType, messageHeaders);

        return apply.map(sp -> AvroDatumReaders.toDatumReader(genericContainerType, sp.writer()
            .orElse(null),
                sp.reader()
                    .orElse(null)))
            .orElseThrow(() -> new IllegalArgumentException("blah"));
    }

    /**
     * To schema pair.
     *
     * @param <T> the generic type
     * @param genericContainerType the generic container type
     * @param messageHeaders the message headers
     * @return the schema pair
     */
    public static final <T extends GenericContainer> SchemaPair toSchemaPair(@NonNull Class<T> genericContainerType,
            @NonNull Map<String, Object> messageHeaders,
            @NonNull BiFunction<? super Class<T>, ? super Map<String, Object>, ? extends SchemaPair> toSchemaPair) {

        return toSchemaPair.apply(genericContainerType, messageHeaders);
    }
}
