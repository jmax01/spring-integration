package org.springframework.integration.avro;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import avro.shaded.com.google.common.base.Objects;

/**
 * The Class AvroSchemas.
 */
public final class AvroSchemas {

    /** The Constant GET_CLASS_SCHEMA_METHOD_NAME. */
    public static final String GET_CLASS_SCHEMA_METHOD_NAME = "getClassSchema";

    /** The Constant SCHEMAS_BY_CLASS. */
    static final Map<Class<? extends GenericContainer>, Schema> SCHEMAS_BY_CLASS = new ConcurrentHashMap<>();

    /**
     * Returns the schema for the supplied {@link GenericContainer} class adding it to the cache in the process.
     *
     * @param clazz the clazz
     * @return the schema or null
     */
    @NonNull
    public static Optional<Schema> toSchema(@NonNull Class<? extends GenericContainer> clazz) {
        return Optional.ofNullable(SCHEMAS_BY_CLASS.computeIfAbsent(clazz,
                irc -> SpecificRecord.class.isAssignableFrom(clazz) || SpecificFixed.class.isAssignableFrom(clazz)
                        ? (Schema) ReflectionUtils.getField(
                                ReflectionUtils.findField(clazz, GET_CLASS_SCHEMA_METHOD_NAME, Schema.class), null)
                        : null));
    }

    /**
     * The Interface SchemaPair.
     */
    public interface SchemaPair {

        /**
         * Factory for creating pairs of Schemas for creation of {@link DatumReader} instances.
         * <p>
         * At least one {@link Schema} must not be {@code null}.
         *
         * @param readerSchema the reader {@link Schema}, may be {@code null} if the {@code writerSchema} is not
         *            {@code null}
         * @param writerSchema the writer {@link Schema}, may be {@code null} if the {@code readerSchema} is not
         *            {@code null}
         * @return the schema pair
         */
        static SchemaPair of(@Nullable Schema readerSchema, @Nullable Schema writerSchema) {

            Assert.isTrue(readerSchema != null || writerSchema != null, "At least one schema must not be null");

            if (readerSchema != null && writerSchema != null) {
                return new CompleteSchemaPair(readerSchema, writerSchema);
            }

            return readerSchema == null ? new WriterSchemaOnly(writerSchema) : new ReaderSchemaOnly(readerSchema);

        }

        /**
         * Factory for creating pairs of Schemas for creation of {@link DatumReader} instances.
         * <p>
         * At least one {@link Schema} must not be {@code null}.
         *
         * @param readerSchemaOnly the reader {@link Schema}, may be {@code null} if the {@code writerSchema} is not
         *            {@code null}
         * @param writerSchemaOnly the writer schema only
         * @return the schema pair
         */
        static SchemaPair of(@Nullable ReaderSchemaOnly readerSchemaOnly, @Nullable WriterSchemaOnly writerSchemaOnly) {

            Assert.isTrue(readerSchemaOnly != null || writerSchemaOnly != null, "At least one schema must not be null");

            if (readerSchemaOnly != null && writerSchemaOnly != null) {
                return new CompleteSchemaPair(readerSchemaOnly.reader, writerSchemaOnly.writer);
            }

            return readerSchemaOnly == null ? writerSchemaOnly : readerSchemaOnly;

        }

        /**
         * Factory for creating {@link ReaderSchemaOnly} instances.
         * <p>
         * At least one {@link Schema} must not be {@code null}.
         *
         * @param readerSchema the reader {@link Schema}, may not be {@code null}
         *            {@code null}
         * @return the reader schema only
         */
        static ReaderSchemaOnly readerOnly(@NonNull Schema readerSchema) {

            Assert.notNull(readerSchema, "'readerSchema' must not be null");

            return new ReaderSchemaOnly(readerSchema);

        }

        /**
         * Factory for creating {@link WriterSchemaOnly} instances.
         * <p>
         * At least one {@link Schema} must not be {@code null}.
         *
         * @param writerSchema the writer {@link Schema}, may not be {@code null}
         *
         * @return the writer schema only
         */
        static WriterSchemaOnly writerOnly(@NonNull Schema writerSchema) {

            Assert.notNull(writerSchema, "'writerSchema' must not be null");

            return new WriterSchemaOnly(writerSchema);

        }

        /**
         * The reader schema
         *
         * @return the optional
         */
        Optional<Schema> reader();

        /**
         * Checks for reader.
         *
         * @return true, if successful
         */
        boolean hasReader();

        /**
         * Writer.
         *
         * @return the optional
         */
        Optional<Schema> writer();

        /**
         * Checks for writer.
         *
         * @return true, if successful
         */
        boolean hasWriter();

    }

    /**
     * The Class ReaderSchemaOnly.
     */
    public static final class ReaderSchemaOnly implements SchemaPair {

        final Schema reader;

        /**
         * Instantiates a ReaderSchemaOnly .
         *
         * @param reader the reader
         */
        public ReaderSchemaOnly(@NonNull Schema reader) {
            this.reader = reader;

        }

        @Override
        @NonNull
        public Optional<Schema> reader() {
            return Optional.of(this.reader);
        }

        @Override
        public boolean hasReader() {

            return true;
        }

        @Override
        @NonNull
        public Optional<Schema> writer() {
            return Optional.empty();
        }

        @Override
        public boolean hasWriter() {
            return false;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) {
                return true;
            }
            if (o == null || !(o instanceof ReaderSchemaOnly)) {
                return false;
            }

            ReaderSchemaOnly that = (ReaderSchemaOnly) o;

            return this.reader.equals(that.reader);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.reader);
        }

        @Override
        public String toString() {
            return "ReaderSchemaOnly [reader=" + this.reader + "]";
        }
    }

    /**
     * The Class WriterSchemaOnly.
     */
    public static class WriterSchemaOnly implements SchemaPair {

        final Schema writer;

        /**
         * Instantiates a WriterSchemaOnly .
         *
         * @param writer the writer
         */
        public WriterSchemaOnly(@NonNull Schema writer) {
            this.writer = writer;

        }

        @Override
        @NonNull
        public Optional<Schema> reader() {
            return Optional.empty();
        }

        @Override
        public boolean hasReader() {

            return false;
        }

        @Override
        @NonNull
        public Optional<Schema> writer() {
            return Optional.empty();
        }

        @Override
        public boolean hasWriter() {
            return false;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) {
                return true;
            }
            if (o == null || !(o instanceof WriterSchemaOnly)) {
                return false;
            }

            WriterSchemaOnly that = (WriterSchemaOnly) o;

            return this.writer.equals(that.writer);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.writer);
        }

        @Override
        public String toString() {
            return "WriterSchemaOnly [writer=" + this.writer + "]";
        }
    }

    /**
     * The Class CompleteSchemaPair.
     */
    public static final class CompleteSchemaPair implements SchemaPair {

        final Schema reader;

        final Schema writer;

        /**
         * Instantiates a new complete schema pair.
         *
         * @param reader the reader
         * @param writer the writer
         */
        public CompleteSchemaPair(@NonNull Schema reader, @NonNull Schema writer) {
            super();
            this.reader = reader;
            this.writer = writer;
        }

        @Override
        @NonNull
        public Optional<Schema> reader() {
            return Optional.of(this.reader);
        }

        @Override
        public boolean hasReader() {

            return true;
        }

        @Override
        @NonNull
        public Optional<Schema> writer() {
            return Optional.of(this.writer);
        }

        @Override
        public boolean hasWriter() {
            return true;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) {
                return true;
            }
            if (o == null || !(o instanceof CompleteSchemaPair)) {
                return false;
            }

            CompleteSchemaPair that = (CompleteSchemaPair) o;

            return this.reader.equals(that.reader) && this.writer.equals(that.writer);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.reader, this.writer);
        }

        @Override
        public String toString() {
            return "CompleteSchemaPair [reader=" + this.reader + ", writer=" + this.writer + "]";
        }
    }
}
