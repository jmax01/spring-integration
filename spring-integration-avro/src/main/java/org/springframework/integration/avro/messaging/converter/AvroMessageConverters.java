package org.springframework.integration.avro.messaging.converter;

import java.util.Collection;

import org.springframework.lang.NonNull;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

/**
 * The Class AvroMessageConverters.
 */
public final class AvroMessageConverters {

    /**
     * The Interface AvroMessageConverter.
     */
    public interface AvroMessageConverter extends MessageConverter {}

    /**
     * The Class AbstractAvroMessageConverter.
     */
    public abstract class AbstractAvroMessageConverter extends AbstractMessageConverter
            implements AvroMessageConverter {

        /**
         * Instantiates a new abstract avro message converter.
         *
         * @param supportedMimeTypes the supported mime types
         */
        protected AbstractAvroMessageConverter(@NonNull Collection<MimeType> supportedMimeTypes) {
            super(supportedMimeTypes);

        }

        /**
         * Instantiates a new abstract avro message converter.
         *
         * @param supportedMimeType the supported mime type
         */
        protected AbstractAvroMessageConverter(@NonNull MimeType supportedMimeType) {
            super(supportedMimeType);
        }

    }
}
