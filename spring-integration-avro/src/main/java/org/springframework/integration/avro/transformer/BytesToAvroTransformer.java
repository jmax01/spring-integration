/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.avro.transformer;

import static org.springframework.integration.avro.io.AvroDecoderFactories.*;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.avro.AvroHeaders;
import org.springframework.integration.avro.AvroSchemas.SchemaPair;
import org.springframework.integration.avro.generic.AvroGenericContainers;
import org.springframework.integration.avro.messaging.AvroMessages;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * An Apache Avro transformer to create generated {@link SpecificRecord} objects
 * from {@code byte[]}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 5.2
 *
 */
public class BytesToAvroTransformer extends AbstractTransformer implements BeanClassLoaderAware {

    @SuppressWarnings("unused")
    private static final Log LOGGER = LogFactory.getLog(AvroToBytesTransformer.class);

    private final DecoderFactory decoderFactory;

    @Nullable
    private final Class<? extends GenericContainer> defaultGenericContainerType;

    @NonNull
    private final BiFunction<? super Class<GenericContainer>, ? super Map<String, Object>, ? extends Optional<SchemaPair>> toSchemaPair;

    private Expression avroJavaClassOrFqcnExpression = new FunctionExpression<Message<?>>(
            (message) -> message.getHeaders()
                .get(AvroHeaders.PAYLOAD_FQCN));

    private EvaluationContext evaluationContext;

    private ClassLoader beanClassLoader;

    /**
     * Instantiates a new BytesToAvroTransformer instance with the supplied {@link DecoderFactory}.
     *
     * @param decoderFactory the decoder factory
     */
    public BytesToAvroTransformer(@NonNull DecoderFactory decoderFactory) {
        this.decoderFactory = decoderFactory;
        this.defaultGenericContainerType = null;
        this.toSchemaPair = (genericContainerType, headers) -> AvroGenericContainers.toSchemaPair(genericContainerType);
    }

    /**
     * Instantiates a new BytesToAvroTransformer instance with the {@link #DEFAULT_DECODER_FACTORY}
     */
    public BytesToAvroTransformer() {
        this(DEFAULT_DECODER_FACTORY);
    }

    /**
     * Instantiates a new BytesToAvroTransformer instance with the supplied default generic container type to create and
     * {@link #DEFAULT_DECODER_FACTORY}.
     *
     * @param defaultGenericContainerType the default generic container type
     * @param toSchemaPair the to schema pair
     */
    public BytesToAvroTransformer(@NonNull Class<? extends GenericContainer> defaultGenericContainerType,
            BiFunction<? super Class<? extends GenericContainer>, ? super Map<String, Object>, ? extends Optional<SchemaPair>> toSchemaPair) {
        Assert.notNull(defaultGenericContainerType, "'defaultGenericContainerType' must not be null");
        this.decoderFactory = DEFAULT_DECODER_FACTORY;
        this.defaultGenericContainerType = defaultGenericContainerType;
        this.toSchemaPair = toSchemaPair;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.beanClassLoader = classLoader;
    }

    /**
     * Set the expression to evaluate against the message to determine the type.
     * Default {@code headers['avro_type']}.
     *
     * @param avroJavaClassOrFqcnExpression the avro java class or fqcn expression
     * @return the transformer
     */
    public BytesToAvroTransformer setAvroJavaClassOrFqcnExpression(@NonNull Expression avroJavaClassOrFqcnExpression) {
        assertExpressionNotNull(avroJavaClassOrFqcnExpression);
        this.avroJavaClassOrFqcnExpression = avroJavaClassOrFqcnExpression;
        return this;
    }

    private void assertExpressionNotNull(Object expression) {
        Assert.notNull(expression, "'expression' must not be null");
    }

    /**
     * Set the expression to evaluate against the message to determine the type id.
     * Default {@code headers['avro_type']}.
     *
     * @param avroJavaClassOrFqcnExpressionAsString the avro java class or fqcn expression as string
     * @return the transformer
     */
    @NonNull
    public BytesToAvroTransformer typeExpression(@NonNull String avroJavaClassOrFqcnExpressionAsString) {
        assertExpressionNotNull(avroJavaClassOrFqcnExpressionAsString);
        this.avroJavaClassOrFqcnExpression = EXPRESSION_PARSER.parseExpression(avroJavaClassOrFqcnExpressionAsString);
        return this;
    }

    /**
     * Set the expression to evaluate against the message to determine the type.
     * Default {@code headers['avro_type']}.
     *
     * @param avroJavaClassOrFqcnExpression the new type expression
     */
    public void setTypeExpression(@NonNull Expression avroJavaClassOrFqcnExpression) {
        assertExpressionNotNull(avroJavaClassOrFqcnExpression);
        this.avroJavaClassOrFqcnExpression = avroJavaClassOrFqcnExpression;
    }

    /**
     * Set the expression to evaluate against the message to determine the type id.
     * Default {@code headers['avro_type']}.
     *
     * @param avroJavaClassOrFqcnExpressionAsString the new type expression
     */
    public void setTypeExpression(@NonNull String avroJavaClassOrFqcnExpressionAsString) {
        assertExpressionNotNull(avroJavaClassOrFqcnExpressionAsString);
        this.avroJavaClassOrFqcnExpression = EXPRESSION_PARSER.parseExpression(avroJavaClassOrFqcnExpressionAsString);
    }

    @Override
    protected void onInit() {
        this.evaluationContext = IntegrationContextUtils.getEvaluationContext(getBeanFactory());
    }

    @Override
    protected Object doTransform(@NonNull Message<?> message) {
        Object payload = message.getPayload();
        Assert.state(payload instanceof byte[], "Payload must be a byte[]");

        Class<? extends GenericContainer> genericContainerClass = toGenericContainerClass(message);

        return AvroGenericContainers.fromBytes((byte[]) payload,
                AvroMessages.toDatumReader(genericContainerClass, message.getHeaders(), this.toSchemaPair),
                this.decoderFactory);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    Class<? extends GenericContainer> toGenericContainerClass(@NonNull Message<?> message) {

        Object avroJavaClassOrFqcn = this.avroJavaClassOrFqcnExpression.getValue(this.evaluationContext, message);

        if (avroJavaClassOrFqcn instanceof Class) {
            Class<?> avroJavaClass = (Class<?>) avroJavaClassOrFqcn;
            avroJavaClass.isAssignableFrom(GenericContainer.class);

            return (Class<? extends GenericContainer>) avroJavaClass;
        }

        if (avroJavaClassOrFqcn instanceof String) {
            try {
                final String avroJavaFqcn = (String) avroJavaClassOrFqcn;
                return (Class<? extends GenericContainer>) ClassUtils.forName(avroJavaFqcn, this.beanClassLoader);
            } catch (ClassNotFoundException | LinkageError e) {
                throw new IllegalStateException(e);
            }
        }

        if (this.defaultGenericContainerType == null) {
            throw new IllegalArgumentException("Could not determine avro generic container type from message headers");
        }
        return this.defaultGenericContainerType;
    }
}
