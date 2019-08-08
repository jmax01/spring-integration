package org.springframework.integration.avro.specific;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

/**
 * The Class SpecificRecords.
 */
public final class SpecificRecords {

    static final ConcurrentHashMap<Class<? extends SpecificRecord>, Schema> CLASS_SCHEMAS_BY_SPECIFIC_RECORD_CLASS = new ConcurrentHashMap<>();

    /**
     * Attempts to retrieve the {@code ClassSchema}, the {@link Schema} statically declared on the
     * {@link SpecificRecord} class
     * first via {@link SpecificTypes#CLASS_SCHEMA_FIELD_NAME} field then via the
     * {@link SpecificTypes#GET_CLASS_SCHEMA_METHOD_NAME}.
     *
     * @param <T> the {@link SpecificRecord} type
     * @param specificRecordClass the {@link SpecificRecord} class from which to retrieve the
     *            class {@link Schema}
     * @return the class {@link Schema}
     */
    public static <T extends SpecificRecord> Schema toClassSchema(@NonNull Class<T> specificRecordClass) {
        Assert.notNull(specificRecordClass, "'specificRecordClass' must not be null");
        return CLASS_SCHEMAS_BY_SPECIFIC_RECORD_CLASS.computeIfAbsent(specificRecordClass,
                _specificRecordClass -> SpecificTypes.toClassSchema(_specificRecordClass));
    }
}
