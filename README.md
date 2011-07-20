# Introduction

postgresql-partitioning adds much needs automatic partition management to PostgreSQL.

# Installation

TODO

# Partitioning Tables

The first step is to create the table you wish to partition. You may use any table you have already
created but the table must have no records.

## By List

1. CREATE TABLE:

    CREATE TABLE mytable (

      id SERIAL PRIMARY KEY,

	  some_value INT NOT NULL,

	  message TEXT

	);

2. Partition the table:

	SELECT create_table_partition_by_list('mytable', 'some_value', '{1,2,3}');

3. INSERT records:

	INSERT INTO mytable (some_value, message) VALUES (2, 'some text');

	INSERT 0 0

	INSERT INTO mytable (some_value, message) VALUES (5, 'this will fail');

	ERROR:  List value out of range.

create_table_partition_by_list() takes three arguments;

1. The name of the table to partition (this table must have no records)

2. The column to use for partitioning.

3. The list values used to create each partition. Since there are three possible values for
   some_value, three respective partitions will be created immediatly.
