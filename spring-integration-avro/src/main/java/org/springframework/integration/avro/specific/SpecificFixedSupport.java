package org.springframework.integration.avro.specific;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificFixed;
import org.springframework.lang.NonNull;

/**
 * The Class SpecificFixedSupport.
 */
public final class SpecificFixedSupport {

    static final ConcurrentHashMap<Class<? extends SpecificFixed>, Schema> CLASS_SCHEMAS_BY_SPECIFIC_FIXED_CLASS = new ConcurrentHashMap<>();

    /**
     * Attempts to retrieve the {@code ClassSchema}, the {@link Schema} statically declared on the
     * {@link SpecificFixed} class
     * first via {@link SpecificTypes#CLASS_SCHEMA_FIELD_NAME} field then via the
     * {@link SpecificTypes#GET_CLASS_SCHEMA_METHOD_NAME}.
     *
     * @param <T> the {@link SpecificFixed} type
     * @param specificFixedClass the {@link SpecificFixed} class from which to retrieve the
     *            class {@link Schema}
     * @return the class {@link Schema}
     */
    public static <T extends SpecificFixed> Schema toClassSchema(@NonNull Class<T> specificFixedClass) {

        return CLASS_SCHEMAS_BY_SPECIFIC_FIXED_CLASS.computeIfAbsent(specificFixedClass,
                _specificFixedClass -> SpecificTypes.toClassSchema(_specificFixedClass));
    }

}
