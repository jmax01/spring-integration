package org.springframework.integration.avro.specific;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

/**
 * The Class SpecificTypes.
 */
public final class SpecificTypes {

    private static final Log LOGGER = LogFactory.getLog(SpecificTypes.class);

    /** The Constant CLASS_SCHEMA_FIELD_NAME. */
    public static final String CLASS_SCHEMA_FIELD_NAME = "SCHEMA$";

    /** The Constant GET_CLASS_SCHEMA_METHOD_NAME. */
    public static final String GET_CLASS_SCHEMA_METHOD_NAME = "getClassSchema";

    /**
     * Attempt to retrieve the {@code ClassSchema}, the {@link Schema} static declared on {@link SpecificRecord} or
     * {@link SpecificFixed}
     * classes first via {@link SpecificTypes#CLASS_SCHEMA_FIELD_NAME} field then via the
     * {@link SpecificTypes#GET_CLASS_SCHEMA_METHOD_NAME}.
     *
     * <p>
     * Since the nearest common interface between {@link SpecificRecord} and {@link SpecificFixed} is
     * {@link GenericContainer}
     * (they don't share a common {@code Specific*} interface) this is method is {@code package private}
     * to prevent users from attempting to retrieve Schemas from other {@link GenericContainer} interfaces
     *
     * @param <T> the {@link SpecificRecord} or {@link SpecificFixed} type
     * @param specificTypeClass the {@link SpecificRecord} or {@link SpecificFixed} class from which to retrieve the
     *            {@link Schema}
     * @return the {@link Schema}
     */
    @Nullable
    static <T extends GenericContainer> Schema toClassSchema(@NonNull Class<T> specificTypeClass) {

        Schema classSchemaViaField = toClassSchemaViaField(specificTypeClass);
        return classSchemaViaField == null ? toClassSchemaViaGetClassSchemaMethod(specificTypeClass)
                : classSchemaViaField;
    }

    /**
     * Attempt to retrieve the {@code ClassSchema}, the {@link Schema} static declared on {@link SpecificRecord} or
     * {@link SpecificFixed}
     * classes via the {@link SpecificTypes#GET_CLASS_SCHEMA_METHOD_NAME}.
     *
     * <p>
     * Since the nearest common interface between {@link SpecificRecord} and {@link SpecificFixed} is
     * {@link GenericContainer}
     * (they don't share a common {@code Specific*} interface) this is method is {@code package private}
     * to prevent users from attempting to retrieve Schemas from other {@link GenericContainer} interfaces
     *
     * @param <T> the {@link SpecificRecord} or {@link SpecificFixed} type
     * @param specificTypeClass the {@link SpecificRecord} or {@link SpecificFixed} class from which to retrieve the
     *            {@link Schema}
     * @return the {@link Schema}
     */
    @Nullable
    static <T extends GenericContainer> Schema toClassSchemaViaField(@NonNull Class<T> specificTypeClass) {

        // Leverage Spring's field cache.
        final Field classSchemaField = ReflectionUtils.findField(specificTypeClass, CLASS_SCHEMA_FIELD_NAME,
                Schema.class);
        if (classSchemaField == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Could not find " + CLASS_SCHEMA_FIELD_NAME + "field on Specific Type class: "
                        + specificTypeClass.getName());
            }
            return null;
        }

        try {
            return (Schema) classSchemaField.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Could not access " + CLASS_SCHEMA_FIELD_NAME + "field on Specific Type class: "
                        + specificTypeClass.getName(), e);
            }
            return null;
        }
    }

    /**
     * Attempt to retrieve the {@code ClassSchema}, the {@link Schema} static declared on {@link SpecificRecord} or
     * {@link SpecificFixed}
     * classes then via the {@link SpecificTypes#GET_CLASS_SCHEMA_METHOD_NAME}.
     *
     * <p>
     * Since the nearest common interface between {@link SpecificRecord} and {@link SpecificFixed} is
     * {@link GenericContainer}
     * (they don't share a common {@code Specific*} interface) this is method is {@code package private}
     * to prevent users from attempting to retrieve Schemas from other {@link GenericContainer} interfaces
     *
     * @param <T> the {@link SpecificRecord} or {@link SpecificFixed} type
     * @param specificTypeClass the {@link SpecificRecord} or {@link SpecificFixed} class from which to retrieve the
     *            {@link Schema}
     * @return the {@link Schema}
     */
    @Nullable
    static <
        T extends GenericContainer> Schema toClassSchemaViaGetClassSchemaMethod(@NonNull Class<T> specificTypeClass) {

        // Leverage Spring's method cache.
        final Method getClassSchemaMethod = ReflectionUtils.findMethod(specificTypeClass, GET_CLASS_SCHEMA_METHOD_NAME);

        if (getClassSchemaMethod == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Could not find " + GET_CLASS_SCHEMA_METHOD_NAME + "method on on Specific Type class: "
                        + specificTypeClass.getName());
            }
            return null;
        }
        try {
            return (Schema) getClassSchemaMethod.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Could not invoke " + GET_CLASS_SCHEMA_METHOD_NAME + "method on Specific Type class: "
                        + specificTypeClass.getName(), e);
            }
            return null;
        }
    }
}
