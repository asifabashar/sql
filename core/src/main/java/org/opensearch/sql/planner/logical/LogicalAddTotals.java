/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;

/**
 * Logical plan for the AddTotals operation. This operation computes totals for numeric fields and
 * appends a totals row to the end of the result set.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAddTotals extends LogicalPlan {

  /** List of fields to compute totals for. If empty, all numeric fields will be totaled. */
  private final List<NamedExpression> fieldList;

  /** Label for the totals row. Default is "Total" if not specified. */
  private final String label;

  /** Field to use for labeling the totals row. If specified, this field will contain the label. */
  private final String labelField;

  /**
   * Constructor for LogicalAddTotals.
   *
   * @param child The child logical plan
   * @param fieldList List of fields to total (empty means all numeric fields)
   * @param label Label for the totals row
   * @param labelField Field name to use for the label
   */
  public LogicalAddTotals(
      LogicalPlan child, List<NamedExpression> fieldList, String label, String labelField) {
    super(Collections.singletonList(child));
    this.fieldList = fieldList;
    this.label = label != null ? label : "Total";
    this.labelField = labelField;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAddTotals(this, context);
  }
}
