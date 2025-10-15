/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * A custom implementation of number/boolean to string .
 *
 * <p>This operator is necessary because tostring has following requirements "binary" Converts a
 * number to a binary value. "hex" Converts the number to a hexadecimal value. "commas" Formats the
 * number with commas. If the number includes a decimal, the function rounds the number to nearest
 * two decimal places. "duration" Converts the value in seconds to the readable time format
 * HH:MM:SS. if not format parameter provided, then consider value as boolean
 */
public class ToNumberFunction extends ImplementorUDF {
    public ToNumberFunction() {
        super(
                new org.opensearch.sql.expression.function.udf.ToNumberFunction.ToNumberImplementor(),
                NullPolicy.ANY);
    }



    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return PPLReturnTypes.STRING_FORCE_NULLABLE;
    }

    @Override
    public UDFOperandMetadata getOperandMetadata() {
        return PPLOperandTypes.STRING_INTEGER;
    }

    public static class ToNumberImplementor implements NotNullImplementor {

        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
                Expression fieldValue = translatedOperands.get(0);
                String fieldValueStr = fieldValue.toString();
                Expression baseExpr = null;

                if (translatedOperands.size()>1) {
                    baseExpr = translatedOperands.get(1);



                }
                    if (fieldValueStr.contains(".")){
                        return Expressions.call(ToNumberFunction.class, "toNumberDouble", fieldValue);
                    } else {
                        return Expressions.call(ToNumberFunction.class, "toNumberInt", fieldValue, baseExpr);
                    }
        }
    }



    @Strict
    public static double toNumberDouble(String fieldValue) {
        return Double.parseDouble(fieldValue);

    }

    @Strict
    public static int toNumberInt(String fieldValue, int base) {
        try {

            if (base<2 || base>36) {
                base = 10;
            }
        } catch (NumberFormatException e) {

        }
           return Integer.parseInt(fieldValue, base);
    }

}