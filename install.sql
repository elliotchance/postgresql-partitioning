CREATE OR REPLACE FUNCTION _partition_init()
RETURNS VOID
AS $$
DECLARE
	counter INT;
BEGIN
	SELECT COUNT(*) INTO counter FROM pg_class WHERE relname = '_partition_table';
	IF(counter > 0) THEN
		RETURN;
	END IF;
	
	-- init
	CREATE TABLE _partition_table (
		table_name TEXT NOT NULL PRIMARY KEY,
		partition_type TEXT NOT NULL,
		expression TEXT NOT NULL
	);
	CREATE TABLE _partition (
		table_name TEXT NOT NULL REFERENCES _partition_table(table_name)
			ON DELETE CASCADE ON UPDATE CASCADE,
		partition_name TEXT NOT NULL,
		test TEXT NOT NULL,
		PRIMARY KEY ( table_name, partition_name )
	);
	
	-- views
	CREATE VIEW pg_partition AS
	SELECT _partition.table_name, _partition.partition_name, _partition_table.partition_type,
		expression, test
	FROM _partition, _partition_table
	WHERE _partition.table_name=_partition_table.table_name;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _table_is_partitioned(
	the_table_name TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
BEGIN
	SELECT COUNT(*) INTO counter FROM _partition_table WHERE table_name=the_table_name;
	RETURN counter > 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_exists(
	the_table_name TEXT,
	the_partition_name TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
BEGIN
	SELECT COUNT(*) INTO counter FROM _partition WHERE table_name=the_table_name AND
		partition_name=the_partition_name;
	RETURN counter > 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _add_partition(
	the_table_name TEXT,
	the_partition_name TEXT,
	the_expression TEXT,
	the_list_value TEXT
)
RETURNS BOOLEAN
AS $$
BEGIN
	-- create partition table
	EXECUTE 'CREATE TABLE ' || the_table_name || '_' || the_partition_name || '( CHECK((' ||
		the_expression || ')::text = (''' || the_list_value || ''')::text) ) INHERITS (' ||
		the_table_name || ')';
		
	-- register partition
	INSERT INTO _partition (table_name, partition_name, test)
	VALUES (the_table_name, the_partition_name, the_list_value);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _rebuild_triggers(
	the_table_name TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	the_trigger TEXT;
	part_row RECORD;
	counter INT;
BEGIN
	-- build trigger function
	the_trigger := 'CREATE OR REPLACE FUNCTION ' || the_table_name || '_insert_trigger() ' ||
		'RETURNS TRIGGER AS $TRIGGER$ BEGIN IF(FALSE) THEN /* do nothing */ ';
		
	FOR part_row IN SELECT * FROM pg_partition WHERE table_name=the_table_name
	LOOP
		the_trigger := the_trigger || ' ELSIF (NEW.' || part_row.expression || '::text = ''' ||
			part_row.test || '''::text) THEN INSERT INTO ' || the_table_name || '_' ||
			part_row.partition_name || ' VALUES (NEW.*); ';
	END LOOP;
	
	the_trigger := the_trigger || 'ELSE RAISE EXCEPTION ''List value out of range.''; END IF;' ||
		' RETURN NULL; END; $TRIGGER$ LANGUAGE plpgsql';
	EXECUTE the_trigger;
	
	-- attach trigger
	SELECT count(*) INTO counter FROM pg_trigger WHERE tgname='insert_' || the_table_name || '_trigger';
	IF(counter = 0) THEN
		the_trigger := 'CREATE TRIGGER insert_' || the_table_name || '_trigger' ||
			' BEFORE INSERT ON ' || the_table_name || ' FOR EACH ROW EXECUTE PROCEDURE ' ||
			the_table_name || '_insert_trigger()';
		EXECUTE the_trigger;
	END IF;
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_partition_by_list(
	the_table_name TEXT,
	the_expression TEXT,
	the_list_values TEXT[]
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
	the_list_value TEXT;
	the_partition_name TEXT;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();

	-- TODO: make sure the table exists
	
	-- make sure the table has not been partitioned already
	IF(_table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is already partitioned.', the_table_name;
	END IF;
	
	-- we can only partition a table that is empty
	EXECUTE 'SELECT COUNT(*) FROM ' || the_table_name INTO counter;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Only empty tables can be partitioned.', the_table_name;
	END IF;
	
	-- register table for partitioning
	INSERT INTO _partition_table (table_name, partition_type, expression)
	VALUES (the_table_name, 'list', the_expression);
	
	FOR the_list_value IN
		SELECT the_list_values[i] FROM generate_series(1, array_upper(the_list_values, 1)) AS i
	LOOP
		the_partition_name := substring(md5(random()::text) for 8);
		PERFORM _add_partition(the_table_name, the_partition_name, the_expression, the_list_value);
	END LOOP;
	
	-- build and attach triggers
	PERFORM _rebuild_triggers(the_table_name);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_partition(
	the_table_name TEXT,
	the_partition_name TEXT
)
RETURNS BOOLEAN
AS $$
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();
	
	-- make sure the table is partitioned
	IF(NOT _table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is not partitioned.', the_table_name;
	END IF;
	
	-- make sure the partition exists
	IF(NOT _partition_exists(the_table_name, the_partition_name)) THEN
		RAISE EXCEPTION 'Table % does not have the partition %.', the_table_name,
			the_partition_name;
	END IF;
	
	-- deregister partition
	DELETE FROM _partition WHERE table_name=the_table_name AND partition_name=the_partition_name;
	
	-- drop the partition
	EXECUTE 'DROP TABLE ' || the_table_name || '_' || the_partition_name;
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_partitioned_table(
	the_table_name TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	the_partition_name TEXT;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();
	
	-- make sure the table is partitioned
	IF(NOT _table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is not partitioned.', the_table_name;
	END IF;
	
	-- drop the partitions first
	FOR the_partition_name IN SELECT partition_name FROM _partition WHERE table_name=the_table_name
	LOOP
		PERFORM drop_partition(the_table_name, the_partition_name);
	END LOOP;
	
	-- drop the partition
	DELETE FROM _partition_table WHERE table_name=the_table_name;
	EXECUTE 'DROP TABLE ' || the_table_name;
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION describe_partitioned_table(
	the_table_name TEXT
)
RETURNS TEXT
AS $$
DECLARE
	des TEXT;
	part RECORD;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();
	
	-- build table overview description
	des := '  Table: ' || the_table_name || E'\n';
	des := des || '   Type: ' ||
		(SELECT partition_type FROM _partition_table WHERE table_name=the_table_name) ||
		E'\n';
	des := des || '  Count: ' ||
		(SELECT count(*) FROM _partition WHERE table_name=the_table_name) ||
		E'\n';
	des := des || E'\n';
	
	-- build indervidual table descriptions
	des := des || E' Partition Name | Approx Rows | Expression\n';
	des := des || E'----------------+-------------+------------\n';
	FOR part IN
		SELECT pg_partition.*, reltuples
		FROM pg_partition, pg_class
		WHERE table_name=the_table_name AND relname=the_table_name
	LOOP
		des := des || ' ' || part.partition_name ||
			substring('              ' for 14 - length(part.partition_name));
		des := des || ' | ';
		des := des || substring('           ' for 11 - length(part.reltuples::text)) ||
			part.reltuples;
		des := des || ' | ';
		des := des || part.expression || '=' || part.test;
		des := des || E'\n';
	END LOOP;
	
	RETURN des;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION add_list_partition(
	the_table_name TEXT,
	the_partition_name TEXT,
	the_value TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
	the_expression TEXT;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();
	
	-- make sure the table exists and is partitioned
	IF(NOT _table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is not partitioned.', the_table_name;
	END IF;
	
	-- make sure this table is LIST partitioned
	IF((SELECT partition_type FROM _partition_table WHERE table_name=the_table_name) != 'list') THEN
		RAISE EXCEPTION 'Table % is not partitioned by list.', the_table_name;
	END IF;
	
	-- make sure the partition does not exist
	SELECT count(*) INTO counter FROM _partition WHERE table_name=the_table_name AND
		partition_name=the_partition_name;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Partition % already exists.', the_partition_name;
	END IF;
	
	-- make sure this list value does not exist
	SELECT count(*) INTO counter FROM _partition WHERE table_name=the_table_name AND
		test=the_value;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Value % already exists in list partition.', the_value;
	END IF;
	
	-- create partition
	SELECT expression INTO the_expression FROM _partition_table WHERE table_name=the_table_name;
	PERFORM _add_partition(the_table_name, the_partition_name, the_expression, the_value);
	
	-- rebuild triggers
	PERFORM _rebuild_triggers(the_table_name);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
