/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.NamedExpression;

/**
 * Physical operator for AddTotals operation. This operator computes totals for numeric fields and
 * appends a totals row to the end of the result set.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
@RequiredArgsConstructor
public class AddTotalsOperator extends PhysicalPlan {

  /** Input physical plan. */
  @Getter private final PhysicalPlan input;

  /** List of fields to compute totals for. If empty, all numeric fields will be totaled. */
  private final List<NamedExpression> fieldList;

  /** Label for the totals row. */
  private final String label;

  /** Field to use for labeling the totals row. */
  private final String labelField;

  /** Iterator over the combined results (original rows + totals row). */
  private Iterator<ExprValue> iterator;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAddTotals(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public void open() {
    super.open();
    input.open();

    // Collect all input rows and compute totals
    List<ExprValue> allRows = new ArrayList<>();

    while (input.hasNext()) {
      ExprValue row = input.next();
      allRows.add(row);
    }

    // Calculate totals by iterating through all collected rows
    ImmutableMap.Builder<String, ExprValue> finalTotalsBuilder = ImmutableMap.builder();

    if (!allRows.isEmpty()) {
      ExprValue firstRowValue = allRows.get(0);
      firstRowValue
          .tupleValue()
          .forEach(
              (fieldName, fieldValue) -> {
                if (shouldIncludeField(fieldName) && isNumericType(fieldValue)) {
                  // Calculate sum for this numeric field
                  double total = 0.0;
                  for (ExprValue row : allRows) {
                    ExprValue value = row.tupleValue().get(fieldName);
                    if (value != null
                        && !value.isNull()
                        && !value.isMissing()
                        && isNumericType(value)) {
                      try {
                        total += value.doubleValue();
                      } catch (Exception e) {
                        // Skip non-numeric values
                      }
                    }
                  }
                  finalTotalsBuilder.put(fieldName, ExprValueUtils.doubleValue(total));
                } else if (shouldSetLabel(fieldName)) {
                  finalTotalsBuilder.put(fieldName, ExprValueUtils.stringValue(label));
                } else {
                  // Non-numeric fields get missing value in totals row
                  finalTotalsBuilder.put(fieldName, ExprValueUtils.LITERAL_MISSING);
                }
              });
    }

    // If no labelField specified and no existing field for label, add default Total field
    if (labelField == null && !allRows.isEmpty()) {
      finalTotalsBuilder.put("Total", ExprValueUtils.stringValue(label));
    }

    // Create the combined result list: original rows + totals row
    List<ExprValue> combinedResults = new ArrayList<>(allRows);
    ImmutableMap<String, ExprValue> totalsMap = finalTotalsBuilder.build();
    if (!totalsMap.isEmpty()) {
      combinedResults.add(ExprTupleValue.fromExprValueMap(totalsMap));
    } else if (allRows.isEmpty()) {
      // Even with no data, create a totals row with just the label
      if (labelField == null) {
        combinedResults.add(
            ExprTupleValue.fromExprValueMap(
                ImmutableMap.of("Total", ExprValueUtils.stringValue(label))));
      }
    }

    iterator = combinedResults.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator != null && iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    if (iterator == null) {
      throw new IllegalStateException("AddTotalsOperator not opened");
    }
    return iterator.next();
  }

  /**
   * Determines if a field should be included in totals calculation. If fieldList is specified, only
   * those fields are included. If fieldList is empty, all numeric fields are included.
   */
  private boolean shouldIncludeField(String fieldName) {
    if (fieldList.isEmpty()) {
      return true; // Include all fields when no specific fields are specified
    }

    // Check if this field is in the specified field list
    return fieldList.stream().anyMatch(namedExpr -> namedExpr.getNameOrAlias().equals(fieldName));
  }

  /** Determines if this field should contain the label value. */
  private boolean shouldSetLabel(String fieldName) {
    if (labelField != null) {
      return fieldName.equals(labelField);
    }
    // If no labelField specified, use the first non-numeric field
    return false; // For now, we'll handle this in the totals calculation
  }

  /** Checks if the given ExprValue represents a numeric type. */
  private boolean isNumericType(ExprValue value) {
    if (value == null || value.isNull() || value.isMissing()) {
      return false;
    }

    ExprCoreType type = (ExprCoreType) value.type();
    return type == ExprCoreType.INTEGER
        || type == ExprCoreType.LONG
        || type == ExprCoreType.FLOAT
        || type == ExprCoreType.DOUBLE;
  }

  public List<NamedExpression> getFieldList() {
    return fieldList;
  }

  public String getLabel() {
    return label;
  }

  public String getLabelField() {
    return labelField;
  }
}
