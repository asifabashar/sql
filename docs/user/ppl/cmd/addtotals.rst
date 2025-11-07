==========
AddTotals
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

The ``addtotals`` command computes the sum of numeric fields and appends a row with the totals to the result.  The command can also add row totals and add a field to store row totals.  This is useful for creating summary reports with subtotals or grand totals.
The ``addtotals`` command only sums numeric fields (integers, floats, doubles). Non-numeric fields in the field list are ignored even if its specified in field-list or in the case of no field-list specified.

Syntax
======

``addtotals [field-list] [label=<string>] [labelfield=<field>] [row=<boolean>] [col=<boolean>] [fieldname=<field>]``

* ``field-list``: Optional. Comma-separated list of numeric fields to sum. If not specified, all numeric fields are summed.
* ``row=<boolean>``: Optional. Calculates total of each row and add a new field with the total. Default is true.
* ``col=<boolean>``: Optional. Calculates total of each column and add a new event at the end of all events with the total. Default is false.
* ``labelfield=<field>``: Optional. Field name to place the label. If it  specifies a non-existing field, adds the field and shows label at the summary event row at this field. This is applicable when col=true.
* ``label=<string>``: Optional. Custom text for the totals row labelfield's label. Default is "Total".  This is applicable when col=true.
* ``fieldname=<field>``: Optional. Calculates total of each row and add a new field to store this total. This is applicable when row=true.


Example 1: Basic Example
=========================

The example shows placing the label in an existing field.

PPL query::

    os> source=accounts | fields firstname, balance | head 3 | addtotals col=true labelfield=firstname;
    fetched rows / total rows = 4/4
    +-----------+---------+
    | firstname | balance |
    +-----------+---------+
    | Amber     | 39225   |
    | Hattie    | 5686    |
    | Nanette   | 32838   |
    | Total     | 77749   |
    +-----------+---------+

Example 2: Adding column totals and adding a summary event with label specified.
=================================================================================

The example shows adding totals after a stats command where final summary event label is 'Sum' and row=true value was used by default when not specified. It also added new field
specified by labelfield as it did not match existing field.


PPL query::

    os> source=accounts | stats count() by gender | addtotals `count()` col=true label='Sum' labelfield='Total';
    fetched rows / total rows = 3/3
    +----------+--------+-------+
    | count()  | gender | Total |
    +----------+--------+-------+
    | 493      | M      | null  |
    | 507      | F      | null  |
    | 1000     | null   | Sum   |
    +----------+--------+-------+

Example 3: With all options
============================

The example shows using addtotals with all options set.

PPL query::

    os> source=accounts | where age > 30 | stats avg(balance) as avg_balance, count() as count by state | head 3 | addtotals avg_balance, count row=true col=true fieldname="Row Total" label='Sum' labelfield='Column Total';
    fetched rows / total rows = 4/4
    +-------------+-------+-------+-----------+--------------|
    | avg_balance | count | state | Row Total | Column Total |
    +-------------+-------+-------+-----------+--------------|
    | 25652.2     | 5     | AL    | 25657.2   | null         |
    | 32460.4     | 10    | AK    | 32470.4   | null         |
    | 29841.6     | 8     | AZ    | 29849.6   | null         |
    | 87954.2     | 23    | null  | 87977.2   | Sum          |
    +-------------+-------+-------+-----------+--------------|




