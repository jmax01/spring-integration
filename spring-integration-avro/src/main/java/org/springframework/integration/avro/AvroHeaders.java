package org.springframework.integration.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.integration.avro.specific.SpecificFixedSupport;
import org.springframework.integration.avro.specific.SpecificRecords;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * Pre-defined names and prefixes for Apache Avro related headers.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Jeff Maxwell
 * @since 5.2
 */
public final class AvroHeaders {

    /**
     * The prefix for Apache Avro specific message headers.
     */
    public static final String PREFIX = "avro_";

    /** The fully qualified class name of the payload. */
    public static final String PAYLOAD_FQCN = PREFIX + "payload_fqcn";

    /**
     * The name of the payload's {@link org.apache.avro.Schema.Type}.
     * <p>
     * Obtained via {@link Schema#getType()}.
     */
    public static final String SCHEMA_TYPE_NAME = PREFIX + "type";

    /**
     * The full name of the payload's schema.
     * <p>
     * Obtained via {@link Schema#getFullName()}.
     *
     * @see org.apache.avro.Schema#getFullName() Schema#getFullName for details.
     */
    public static final String SCHEMA_FULLNAME = PREFIX + "schema_full_name";

    /**
     * The name of the payload's schema.
     * <p>
     * Obtained via {@link Schema#getName()}.
     *
     * @see org.apache.avro.Schema#getName() Schema#getName for details.
     */
    public static final String SCHEMA_NAME = PREFIX + "schema_name";

    /**
     * The namespace of the payload's schema, if the schema is either a {@link org.apache.avro.Schema.Type#RECORD}
     * , {@link org.apache.avro.Schema.Type#ENUM} or {@link org.apache.avro.Schema.Type#FIXED} type.
     * <p>
     * Obtained via {@link Schema#getNamespace()}.
     *
     * @see org.apache.avro.Schema#getNamespace() Schema#getNamespace for details.
     */
    public static final String SCHEMA_NAMESPACE = PREFIX + "schema_namespace";

    private AvroHeaders() {
        super();
    }

    /**
     * To headers.
     *
     * @param genericContainer the generic container
     * @return the map
     */
    @NonNull
    public static final Map<String, ?> genericContainerToHeaders(@NonNull GenericContainer genericContainer) {
        Map<String, Object> avroHeaders = new HashMap<>();
        avroHeaders.put(PAYLOAD_FQCN, genericContainer.getClass()
            .getName());

        return addSchemaHeaders(genericContainer.getSchema(), avroHeaders);
    }

    /**
     * Specific record class to headers.
     *
     * @param <T> the {@link SpecificRecord} type
     * @param specificRecordClass the {@link SpecificRecord} class
     * @return the map of headers
     */
    @NonNull
    public static final <
        T extends SpecificRecord> Map<String, ?> specificRecordClassToHeaders(@NonNull Class<T> specificRecordClass) {
        Schema classSchema = SpecificRecords.toClassSchema(specificRecordClass);
        return toHeaders(classSchema);
    }

    /**
     * Specific fixed class to headers.
     *
     * @param <T> the {@link SpecificFixed} type
     * @param specificFixedClass the {@link SpecificFixed} class
     * @return the map of headers
     */
    @NonNull
    public static final <
        T extends SpecificFixed> Map<String, ?> specificFixedClassToHeaders(@NonNull Class<T> specificFixedClass) {
        Schema classSchema = SpecificFixedSupport.toClassSchema(specificFixedClass);
        return toHeaders(classSchema);
    }

    /**
     * To headers.
     *
     * @param schema the schema
     * @return the map of headers
     */
    @NonNull
    public static final Map<String, ?> toHeaders(@NonNull Schema schema) {
        return addSchemaHeaders(schema, null);
    }

    @NonNull
    static final Map<String, ?> addSchemaHeaders(@NonNull Schema schema, @Nullable Map<String, Object> headers) {

        Map<String, Object> result = headers == null ? new HashMap<>() : headers;
        result.put(SCHEMA_NAME, schema.getName());
        result.put(SCHEMA_FULLNAME, schema.getFullName());
        Type schemaType = schema.getType();
        result.put(SCHEMA_TYPE_NAME, schemaType.getName());
        switch (schemaType) {
            case RECORD:
            case ENUM:
            case FIXED:
                result.put(SCHEMA_NAMESPACE, schema.getNamespace());
                break;
            default:
                break;
        }

        return result;
    }

    // @NonNull
    // Class<GenericContainer> toGenericContainerClass(@NonNull Map<String, Object> headers) {
    //
    // }

    /**
     * To scheama.
     *
     * @param headers the headers
     * @return the schema
     */
    @NonNull
    Schema toScheama(@NonNull Map<String, Object> headers) {

        return null;

    }

    /**
     * To.
     *
     * @param headers the headers
     * @return the schema
     */
    @NonNull
    Schema to(@NonNull Map<String, Object> headers) {

        return null;

    }
}
