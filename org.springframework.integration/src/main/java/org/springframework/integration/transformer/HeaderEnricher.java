/*
 * Copyright 2002-2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.transformer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.core.Message;
import org.springframework.integration.core.MessagingException;
import org.springframework.integration.message.MessageBuilder;
import org.springframework.util.Assert;

/**
 * A Transformer that adds statically configured header values to a Message.
 * Accepts the boolean 'overwrite' property that specifies whether values
 * should be overwritten. By default, any existing header values for
 * a given key, will <em>not</em> be replaced.
 * 
 * @author Mark Fisher
 */
public class HeaderEnricher implements Transformer {

	private final Map<String, Object> headersToAdd;

	private volatile boolean overwrite;


	/**
	 * Create a HeaderEnricher with the given map of headers.
	 */
	public HeaderEnricher(Map<String, Object> headersToAdd) {
		Assert.notNull(headersToAdd, "headersToAdd must not be null");
		this.headersToAdd = headersToAdd;
	}


	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public Message<?> transform(Message<?> message) {
		try {
			Map<String, Object> headerMap = new HashMap<String, Object>(message.getHeaders());
			for (Map.Entry<String, Object> entry : this.headersToAdd.entrySet()) {
				String key = entry.getKey();
				if (this.overwrite || headerMap.get(key) == null) {
					Object value = entry.getValue();
					if (value instanceof ExpressionHolder) {
						value = ((ExpressionHolder) value).evaluate(message);
					}
					headerMap.put(key, value);
				}
			}
	        return MessageBuilder.withPayload(message.getPayload()).copyHeaders(headerMap).build();
        }
		catch (Exception e) {
        	throw new MessagingException(message, "failed to transform message headers", e);
        }
	}


	public static class ExpressionHolder {

		private static final ExpressionParser parser = new SpelExpressionParser();


		private final String expressionString;

		private final Class<?> expectedType;

		private volatile Expression parsedExpression;


		/**
		 * Create a holder object for the given expression String and the expected type
		 * of the expression evaluation result. The expectedType may be null if unknown.
		 */
		public ExpressionHolder(String expressionString, Class<?> expectedType) {
			this.expressionString = expressionString;
			this.expectedType = expectedType;
		}


		private Object evaluate(Message<?> message) throws ParseException, EvaluationException {
			if (this.parsedExpression == null) {
				synchronized (this) {
					this.parsedExpression = parser.parseExpression(this.expressionString);
				}
			}
			StandardEvaluationContext context = new StandardEvaluationContext(message);
			context.addPropertyAccessor(new MapAccessor());
			return (this.expectedType != null)
					? this.parsedExpression.getValue(context, this.expectedType)
					: this.parsedExpression.getValue(context);
		}
	}

}
