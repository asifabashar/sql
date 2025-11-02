/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLToNumberFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLToNumberFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testNumberBinary() {
    String ppl = "source=EMP | eval int_value = tonumber('010101',2) | fields int_value|head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
            "LogicalSort(fetch=[1])\n  LogicalProject(int_value=[TONUMBER('010101':VARCHAR, 2)])\n    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "int_value=21.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT `TONUMBER`('010101', 2) `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }


    @Test
    public void testNumberHex() {
        String ppl = "source=EMP | eval int_value = tonumber('FA34',16) | fields int_value|head 1";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalSort(fetch=[1])\n  LogicalProject(int_value=[TONUMBER('FA34':VARCHAR, 16)])\n    LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "int_value=64052.0\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql = "SELECT `TONUMBER`('FA34', 16) `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
        verifyPPLToSparkSQL(root, expectedSparkSql);
    }

    @Test
    public void testNumber() {
        String ppl = "source=EMP | eval int_value = tonumber('4598') | fields int_value|head 1";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalSort(fetch=[1])\n  LogicalProject(int_value=[TONUMBER('4598':VARCHAR)])\n    LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "int_value=4598.0\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql = "SELECT `TONUMBER`('4598') `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
        verifyPPLToSparkSQL(root, expectedSparkSql);
    }

    @Test
    public void testNumberDecimal() {
        String ppl = "source=EMP | eval int_value = tonumber('4598.54922') | fields int_value|head 1";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalSort(fetch=[1])\n  LogicalProject(int_value=[TONUMBER('4598.54922':VARCHAR)])\n    LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "int_value=4598.54922\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql = "SELECT `TONUMBER`('4598.54922') `int_value`\nFROM `scott`.`EMP`\nLIMIT 1";
        verifyPPLToSparkSQL(root, expectedSparkSql);
    }
}
