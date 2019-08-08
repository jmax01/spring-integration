package org.springframework.integration.avro;

import org.springframework.util.MimeType;

/**
 * The Class AvroMimeTypes.
 */
public final class AvroMimeTypes {

    /** The Constant APPLICATION_AVRO_SUBTYPE_VALUE. */
    public static final String APPLICATION_AVRO_SUBTYPE_VALUE = "avro";

    /** The Constant APPLICATION_AVRO. */
    public static final MimeType APPLICATION_AVRO = new MimeType("application", APPLICATION_AVRO_SUBTYPE_VALUE);

    /**
     * A String equivalent of {@link AvroMimeTypes#APPLICATION_AVRO}.
     */
    public static final String APPLICATION_AVRO_VALUE = "application/" + APPLICATION_AVRO_SUBTYPE_VALUE;

    /** The Constant AVRO_BINARY_TYPE. */
    public static final String AVRO_BINARY_TYPE = "avro";

    /** The Constant AVRO_BINARY_SUBTYPE. */
    public static final String AVRO_BINARY_SUBTYPE = "binary";

    /** The AVRO_BINARY MimeType */
    public static final MimeType AVRO_BINARY = new MimeType(AVRO_BINARY_TYPE, AVRO_BINARY_SUBTYPE);

    /**
     * A String equivalent of {@link AvroMimeTypes#AVRO_BINARY}.
     */
    public static final String AVRO_BINARY_VALUE = AVRO_BINARY_TYPE + "/" + AVRO_BINARY_SUBTYPE;

}
