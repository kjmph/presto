============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ]
    table_name (
      { column_name data_type [NOT NULL] [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
      | LIKE existing_table_name [ { INCLUDING | EXCLUDING } PROPERTIES ]
      | [ CONSTRAINT constraint_name ] { PRIMARY KEY | UNIQUE } ( { column_name [, ...] } ) [ { ENABLED | DISABLED } ] [ [ NOT ] RELY ] [ [ NOT ] ENFORCED ] }
      | [ CONSTRAINT constraint_name ] FOREIGN KEY ( { column_name [, ...] } ) REFERENCES referenced_table_name ( { referenced_column_name [, ...] } ) [ { ENABLED | DISABLED } ] [ [ NOT ] RELY ] [ [ NOT ] ENFORCED ] }
      [, ...]
    )
    [ COMMENT table_comment ]
    [ WITH ( property_name = expression [, ...] ) ]


Description
-----------

Create a new, empty table with the specified columns.
Use :doc:`create-table-as` to create a table with data.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the table already exists.

The optional ``WITH`` clause can be used to set properties
on the newly created table or on single columns.  To list all available table
properties, run the following query::

    SELECT * FROM system.metadata.table_properties

To list all available column properties, run the following query::

    SELECT * FROM system.metadata.column_properties

The ``LIKE`` clause can be used to include all the column definitions from
an existing table in the new table. Multiple ``LIKE`` clauses may be
specified, which allows copying the columns from multiple tables.

If ``INCLUDING PROPERTIES`` is specified, all of the table properties are
copied to the new table. If the ``WITH`` clause specifies the same property
name as one of the copied properties, the value from the ``WITH`` clause
will be used. The default behavior is ``EXCLUDING PROPERTIES``. The
``INCLUDING PROPERTIES`` option maybe specified for at most one table.

Table constraints may be marked ``ENABLED`` or ``DISABLED``, ``RELY`` or
``NOT RELY``, and ``ENFORCED`` or ``NOT ENFORCED``. Connector support for
storing, enforcing, and using constraints during planning depends on the
connector. A ``RELY`` constraint is trusted metadata for query planning when
the connector supports it, even if the constraint is not enforced by Presto.
Only mark a constraint as ``RELY`` when the data is guaranteed to satisfy the
constraint outside of Presto.


Examples
--------

Create a new table ``orders``::

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC')

Create the table ``orders`` if it does not already exist, adding a table comment,
a column comment, a not null constraint on column ``orderstatus``, and a primary
key constraint on column ``orderkey``::

    CREATE TABLE IF NOT EXISTS orders (
      orderkey bigint,
      orderstatus varchar NOT NULL,
      totalprice double COMMENT 'Price in cents.',
      orderdate date,
      PRIMARY KEY (orderkey)
    )
    COMMENT 'A table to keep track of orders.'

Create the table ``lineitem`` with a disabled foreign key constraint on
``orderkey``::

    CREATE TABLE lineitem (
      orderkey bigint,
      linenumber bigint,
      CONSTRAINT fk_lineitem_orders FOREIGN KEY (orderkey) REFERENCES orders (orderkey) DISABLED RELY NOT ENFORCED
    )

Create the table ``bigger_orders`` using the columns from ``orders``
plus additional columns at the start and end::

    CREATE TABLE bigger_orders (
      another_orderkey bigint,
      LIKE orders,
      another_orderdate date
    )

See Also
--------

:doc:`alter-table`, :doc:`drop-table`, :doc:`create-table-as`, :doc:`show-create-table`
