==========
AddTotals
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

The ``addtotals`` command computes the sum of numeric fields and appends a row with the totals to the result. This is useful for creating summary reports with subtotals or grand totals.

Syntax
======

``addtotals [field-list] [label=<string>] [labelfield=<field>]``

* ``field-list``: Optional. Comma-separated list of numeric fields to sum. If not specified, all numeric fields are summed.
* ``label=<string>``: Optional. Custom text for the totals row label. Default is "Total".  
* ``labelfield=<field>``: Optional. Field name to place the label. If not specified, creates a new field named "Total".

Example 1: Basic totals
=======================

The example shows adding totals for all numeric fields.



Example 1: Basic Example
=========================

The example shows placing the label in an existing field.

PPL query::

    os> source=accounts | fields firstname, balance | head 3 | addtotals labelfield=firstname;
    fetched rows / total rows = 4/4
    +-----------+---------+
    | firstname | balance |
    +-----------+---------+
    | Amber     | 39225   |
    | Hattie    | 5686    |
    | Nanette   | 32838   |
    | Total     | 77749   |
    +-----------+---------+

Example 2: After aggregations
=============================

The example shows adding totals after a stats command.

PPL query::

    os> source=accounts | stats count() by gender | addtotals `count()`;
    fetched rows / total rows = 3/3
    +----------+--------+-------+
    | count()  | gender | Total |
    +----------+--------+-------+
    | 493      | M      | null  |
    | 507      | F      | null  |
    | 1000     | null   | Total |
    +----------+--------+-------+

Example 3: Complex pipeline
===========================

The example shows using addtotals in a complex data processing pipeline.

PPL query::

    os> source=accounts | where age > 30 | stats avg(balance) as avg_balance, count() as count by state | head 3 | addtotals avg_balance, count;
    fetched rows / total rows = 4/4
    +-------------+-------+-------+-------+
    | avg_balance | count | state | Total |
    +-------------+-------+-------+-------+
    | 25652.2     | 5     | AL    | null  |
    | 32460.4     | 10    | AK    | null  |
    | 29841.6     | 8     | AZ    | null  |
    | 87954.2     | 23    | null  | Total |
    +-------------+-------+-------+-------+

Limitations
===========

1. The ``addtotals`` command only sums numeric fields (integers, floats, doubles). Non-numeric fields in the field list are ignored.

2. The totals row is always appended as the last row in the result set.

3. When using ``labelfield``, the specified field must exist in the result set.

4. Field names with special characters must be enclosed in backticks when specified in the field list.