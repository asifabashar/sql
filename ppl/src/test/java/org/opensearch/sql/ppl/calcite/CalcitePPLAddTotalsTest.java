/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class CalcitePPLAddTotalsTest extends CalcitePPLAbstractTest {

  public CalcitePPLAddTotalsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

    @Test
    public void testAddTotals() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL ";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], Total=[$5])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; Total=800.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; Total=1600.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1250.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; Total=2975.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1250.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; Total=2850.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; Total=2450.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3000.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; Total=5000.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; Total=1500.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; Total=1100.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; Total=950.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3000.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; Total=1300.00\n";

        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `SAL` `Total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsAllFields() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals  ";
        RelNode root = getRelNode(ppl);
        String expectedLogical =  "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], Total=[+($7, $5)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; Total=820.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; Total=1630.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; Total=2995.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; Total=2880.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; Total=2460.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; Total=5010.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; Total=1530.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; Total=1120.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; Total=980.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; Total=1310.00\n";

        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `DEPTNO` + `SAL` `Total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsMultiFields() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals DEPTNO SAL ";
        RelNode root = getRelNode(ppl);
        String expectedLogical =  "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], Total=[+($7, $5)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; Total=820.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; Total=1630.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; Total=2995.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; Total=2880.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; Total=2460.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; Total=5010.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; Total=1530.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; Total=1120.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; Total=980.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; Total=1310.00\n";

        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `DEPTNO` + `SAL` `Total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    public void testAddTotalsWithFieldname() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL  fieldname='CustomSum' ";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], CustomSum=[$5])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; CustomSum=800.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; CustomSum=1600.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; CustomSum=2975.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; CustomSum=2850.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; CustomSum=2450.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; CustomSum=5000.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; CustomSum=1500.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; CustomSum=1100.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; CustomSum=950.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; CustomSum=1300.00\n" ;

        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `SAL` `CustomSum`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }
    @Test
    public void testAddTotalsWithFieldnameRowOptionTrue() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL  fieldname='CustomSum' row=true ";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], CustomSum=[$5])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; CustomSum=800.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; CustomSum=1600.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; CustomSum=2975.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; CustomSum=2850.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; CustomSum=2450.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; CustomSum=5000.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; CustomSum=1500.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; CustomSum=1100.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; CustomSum=950.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; CustomSum=1300.00\n" ;

        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `SAL` `CustomSum`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsWithFieldnameRowOptionFalse() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL  fieldname='CustomSum' row=false ";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n  LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult ="DEPTNO=20; SAL=800.00; JOB=CLERK\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN\nDEPTNO=20; SAL=2975.00; JOB=MANAGER\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN\nDEPTNO=30; SAL=2850.00; JOB=MANAGER\nDEPTNO=10; SAL=2450.00; JOB=MANAGER\nDEPTNO=20; SAL=3000.00; JOB=ANALYST\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN\nDEPTNO=20; SAL=1100.00; JOB=CLERK\nDEPTNO=30; SAL=950.00; JOB=CLERK\nDEPTNO=20; SAL=3000.00; JOB=ANALYST\nDEPTNO=10; SAL=1300.00; JOB=CLERK\n";

        verifyResult(root, expectedResult);

        String expectedSparkSql = "SELECT `DEPTNO`, `SAL`, `JOB`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsWithColTrueNoSummaryLabel() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL col=true";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalUnion(all=[true])\n  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], Total=[$5])\n    LogicalTableScan(table=[[scott, EMP]])\n  LogicalProject(DEPTNO=[null:NULL], SAL=[$0], JOB=[null:NULL], Total=[null:NULL])\n    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n      LogicalProject(SAL=[$5])\n        LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; Total=800.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; Total=1600.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1250.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; Total=2975.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1250.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; Total=2850.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; Total=2450.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3000.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; Total=5000.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; Total=1500.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; Total=1100.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; Total=950.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3000.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; Total=1300.00\nDEPTNO=null; SAL=29025.00; JOB=null; Total=null\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql =   "SELECT `DEPTNO`, `SAL`, `JOB`, `SAL` `Total`\nFROM `scott`.`EMP`\nUNION ALL\nSELECT NULL `DEPTNO`, SUM(`SAL`) `SAL`, NULL `JOB`, NULL `Total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }
    @Test
    public void testAddTotalsWithColTrueRowFalseNoSummaryLabel() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL col=true row=false";
        RelNode root = getRelNode(ppl);
        String expectedLogical =  "LogicalUnion(all=[true])\n  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n    LogicalTableScan(table=[[scott, EMP]])\n  LogicalProject(DEPTNO=[null:NULL], SAL=[$0], JOB=[null:NULL])\n    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n      LogicalProject(SAL=[$5])\n        LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN\nDEPTNO=20; SAL=2975.00; JOB=MANAGER\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN\nDEPTNO=30; SAL=2850.00; JOB=MANAGER\nDEPTNO=10; SAL=2450.00; JOB=MANAGER\nDEPTNO=20; SAL=3000.00; JOB=ANALYST\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN\nDEPTNO=20; SAL=1100.00; JOB=CLERK\nDEPTNO=30; SAL=950.00; JOB=CLERK\nDEPTNO=20; SAL=3000.00; JOB=ANALYST\nDEPTNO=10; SAL=1300.00; JOB=CLERK\nDEPTNO=null; SAL=29025.00; JOB=null\n";

        verifyResult(root, expectedResult);

        String expectedSparkSql =   "SELECT `DEPTNO`, `SAL`, `JOB`\nFROM `scott`.`EMP`\nUNION ALL\nSELECT NULL `DEPTNO`, SUM(`SAL`) `SAL`, NULL `JOB`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsWithAllOptionsIncludingFieldname() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL label='GrandTotal' fieldname='CustomSum' labelfield='all_emp_total' row=true col=true";
        RelNode root = getRelNode(ppl);
        String expectedLogical = "LogicalUnion(all=[true])\n  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], CustomSum=[$5], all_emp_total=[null:NULL])\n    LogicalTableScan(table=[[scott, EMP]])\n  LogicalProject(DEPTNO=[null:NULL], SAL=[$0], JOB=[null:NULL], CustomSum=[null:NULL], all_emp_total=['GrandTotal'])\n    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n      LogicalProject(SAL=[$5])\n        LogicalTableScan(table=[[scott, EMP]])\n";
        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; CustomSum=800.00; all_emp_total=null\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; CustomSum=1600.00; all_emp_total=null\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00; all_emp_total=null\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; CustomSum=2975.00; all_emp_total=null\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; CustomSum=1250.00; all_emp_total=null\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; CustomSum=2850.00; all_emp_total=null\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; CustomSum=2450.00; all_emp_total=null\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00; all_emp_total=null\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; CustomSum=5000.00; all_emp_total=null\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; CustomSum=1500.00; all_emp_total=null\nDEPTNO=20; SAL=1100.00; JOB=CLERK; CustomSum=1100.00; all_emp_total=null\nDEPTNO=30; SAL=950.00; JOB=CLERK; CustomSum=950.00; all_emp_total=null\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; CustomSum=3000.00; all_emp_total=null\nDEPTNO=10; SAL=1300.00; JOB=CLERK; CustomSum=1300.00; all_emp_total=null\nDEPTNO=null; SAL=29025.00; JOB=null; CustomSum=null; all_emp_total=GrandTotal\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `SAL` `CustomSum`, NULL `all_emp_total`\nFROM `scott`.`EMP`\nUNION ALL\nSELECT NULL `DEPTNO`, SUM(`SAL`) `SAL`, NULL `JOB`, NULL `CustomSum`, 'GrandTotal' `all_emp_total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }

    @Test
    public void testAddTotalsMatchingLabelFieldWithExisting() throws IOException {
        String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addtotals SAL DEPTNO col=true label='GrandTotal' labelfield='JOB' ";
        // default is row=true for addtotals
        RelNode root = getRelNode(ppl);
        String expectedLogical =   "LogicalUnion(all=[true])\n  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2], Total=[+($7, $5)])\n    LogicalTableScan(table=[[scott, EMP]])\n  LogicalProject(DEPTNO=[$0], SAL=[$1], JOB=['GrandTotal'], Total=[null:NULL])\n    LogicalAggregate(group=[{}], DEPTNO=[SUM($0)], SAL=[SUM($1)])\n      LogicalProject(DEPTNO=[$7], SAL=[$5])\n        LogicalTableScan(table=[[scott, EMP]])\n";

        verifyLogical(root, expectedLogical);
        String expectedResult = "DEPTNO=20; SAL=800.00; JOB=CLERK; Total=820.00\nDEPTNO=30; SAL=1600.00; JOB=SALESMAN; Total=1630.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=20; SAL=2975.00; JOB=MANAGER; Total=2995.00\nDEPTNO=30; SAL=1250.00; JOB=SALESMAN; Total=1280.00\nDEPTNO=30; SAL=2850.00; JOB=MANAGER; Total=2880.00\nDEPTNO=10; SAL=2450.00; JOB=MANAGER; Total=2460.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=5000.00; JOB=PRESIDENT; Total=5010.00\nDEPTNO=30; SAL=1500.00; JOB=SALESMAN; Total=1530.00\nDEPTNO=20; SAL=1100.00; JOB=CLERK; Total=1120.00\nDEPTNO=30; SAL=950.00; JOB=CLERK; Total=980.00\nDEPTNO=20; SAL=3000.00; JOB=ANALYST; Total=3020.00\nDEPTNO=10; SAL=1300.00; JOB=CLERK; Total=1310.00\nDEPTNO=310; SAL=29025.00; JOB=GrandTotal; Total=null\n";
        verifyResult(root, expectedResult);

        String expectedSparkSql =  "SELECT `DEPTNO`, `SAL`, `JOB`, `DEPTNO` + `SAL` `Total`\nFROM `scott`.`EMP`\nUNION ALL\nSELECT SUM(`DEPTNO`) `DEPTNO`, SUM(`SAL`) `SAL`, 'GrandTotal' `JOB`, NULL `Total`\nFROM `scott`.`EMP`";
        verifyPPLToSparkSQL(root, expectedSparkSql);

    }
}
