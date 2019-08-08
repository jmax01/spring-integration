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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.integration.avro.AvroHeaders.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 5.2
 *
 */
@SpringJUnitConfig
class AvroTests {

    @Test
    void testTransformers(@Autowired Config config) {
        AvroTestClass1 test = new AvroTestClass1("baz", "fiz");
        config.in1()
            .send(new GenericMessage<>(test));
        assertThat(config.tapped()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isInstanceOf(byte[].class);
        assertThat(config.out()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isEqualTo(test)
                .isNotSameAs(test);
    }

    @Test
    void testMultiTypeTransformers(@Autowired Config config) {
        AvroTestClass1 test = new AvroTestClass1("baz", "fiz");
        config.in2()
            .send(new GenericMessage<>(test));
        assertThat(config.tapped()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isInstanceOf(byte[].class);
        assertThat(config.out()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isNotEqualTo(test)
                .isInstanceOf(AvroTestClass2.class);
    }

    @Test
    void testMultiTypeTransformersClassName(@Autowired Config config) {
        AvroTestClass1 test = new AvroTestClass1("baz", "fiz");
        config.in3()
            .send(new GenericMessage<>(test));
        assertThat(config.tapped()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isInstanceOf(byte[].class);
        assertThat(config.out()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isNotEqualTo(test)
                .isInstanceOf(AvroTestClass2.class);
    }

    @Test
    void testTransformersNoHeaderPresent(@Autowired Config config) {
        AvroTestClass1 test = new AvroTestClass1("baz", "fiz");
        config.in4()
            .send(new GenericMessage<>(test));
        assertThat(config.tapped()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isInstanceOf(byte[].class);
        assertThat(config.out()
            .receive(0)).isNotNull()
                .extracting(msg -> msg.getPayload())
                .isEqualTo(test)
                .isNotSameAs(test);
    }

    @Configuration
    @EnableIntegration
    static class Config {

        @Bean
        IntegrationFlow flow1() {
            return IntegrationFlows.from(in1())
                .transform(new AvroToBytesTransformer())
                .wireTap(tapped())
                .transform(transformer())
                .channel(out())
                .get();
        }

        @Bean
        IntegrationFlow flow2() {
            return IntegrationFlows.from(in2())
                .transform(new AvroToBytesTransformer())
                .wireTap(tapped())
                .enrichHeaders(h -> h.header(PAYLOAD_FQCN, AvroTestClass2.class, true))
                .transform(transformer())
                .channel(out())
                .get();
        }

        @Bean
        IntegrationFlow flow3() {
            return IntegrationFlows.from(in3())
                .transform(new AvroToBytesTransformer())
                .wireTap(tapped())
                .enrichHeaders(h -> h.header(PAYLOAD_FQCN, AvroTestClass2.class.getName(), true))
                .transform(transformer())
                .channel(out())
                .get();
        }

        @Bean
        IntegrationFlow flow4() {
            return IntegrationFlows.from(in4())
                .transform(new AvroToBytesTransformer())
                .wireTap(tapped())
                .enrichHeaders(h -> h.header(PAYLOAD_FQCN, null, true)
                    .shouldSkipNulls(false))
                .transform(transformer())
                .channel(out())
                .get();
        }

        @Bean
        BytesToAvroTransformer transformer() {
            return new BytesToAvroTransformer(AvroTestClass1.class);
        }

        @Bean
        DirectChannel in1() {
            return new DirectChannel();
        }

        @Bean
        DirectChannel in2() {
            return new DirectChannel();
        }

        @Bean
        DirectChannel in3() {
            return new DirectChannel();
        }

        @Bean
        DirectChannel in4() {
            return new DirectChannel();
        }

        @Bean
        PollableChannel tapped() {
            return new QueueChannel();
        }

        @Bean
        PollableChannel out() {
            return new QueueChannel();
        }

    }

}
