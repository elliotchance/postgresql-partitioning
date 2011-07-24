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
		test_lower TEXT NOT NULL,
		test_upper TEXT,
		PRIMARY KEY ( table_name, partition_name )
	);
	
	-- views
	CREATE VIEW pg_partition AS
	SELECT _partition.table_name, _partition.partition_name, _partition_table.partition_type,
		expression, test_lower, test_upper
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
	the_min_value TEXT,
	the_max_value TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	compare_type TEXT;
BEGIN
	-- get compare type
	compare_type := _get_comparator_type(the_table_name);

	-- create partition table
	IF(the_max_value IS NULL) THEN
		EXECUTE 'CREATE TABLE ' || the_table_name || '_' || the_partition_name || '( CHECK((' ||
			the_expression || ')::' || compare_type || ' = (''' || the_min_value || ''')::' ||
			compare_type || ') ) INHERITS (' || the_table_name || ')';
	ELSE
		EXECUTE 'CREATE TABLE ' || the_table_name || '_' || the_partition_name || '( CHECK((' ||
			the_expression || ')::' || compare_type || ' BETWEEN (''' || the_min_value || ''')::' ||
			compare_type || ' AND (''' || the_max_value || ''')::' || compare_type ||
			') ) INHERITS (' || the_table_name || ')';
	END IF;
		
	-- register partition
	INSERT INTO _partition (table_name, partition_name, test_lower, test_upper)
	VALUES (the_table_name, the_partition_name, the_min_value, the_max_value);
	
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
	the_trigger_name TEXT;
	compare_type TEXT;
BEGIN
	-- get comparator
	compare_type := _get_comparator_type(the_table_name);

	-- build trigger function
	the_trigger := 'CREATE OR REPLACE FUNCTION ' || the_table_name || '_insert_trigger() ' ||
		'RETURNS TRIGGER AS $TRIGGER$ BEGIN IF(FALSE) THEN /* do nothing */ ';
	
	FOR part_row IN SELECT * FROM pg_partition WHERE table_name=the_table_name
	LOOP
		IF(part_row.test_upper IS NULL) THEN
			the_trigger := the_trigger || ' ELSIF (NEW.' || part_row.expression || '::' ||
				compare_type || ' = ''' || part_row.test_lower || '''::' || compare_type ||
				') THEN INSERT INTO ' || the_table_name || '_' || part_row.partition_name ||
				' VALUES (NEW.*); ';
		ELSE
			the_trigger := the_trigger || ' ELSIF (NEW.' || part_row.expression || '::' ||
				compare_type || ' BETWEEN ''' || part_row.test_lower || '''::' || compare_type ||
				' AND ''' || part_row.test_upper || '''::' || compare_type ||
				') THEN INSERT INTO ' || the_table_name || '_' || part_row.partition_name ||
				' VALUES (NEW.*); ';
		END IF;
	END LOOP;
	
	the_trigger := the_trigger || 'ELSE RAISE EXCEPTION ''Value provided for ' ||
		(SELECT partition_type FROM _partition_table WHERE table_name=the_table_name) ||
		' partition is out of range.''; END IF;' ||
		' RETURN NULL; END; $TRIGGER$ LANGUAGE plpgsql';
	EXECUTE the_trigger;
	
	-- attach trigger
	SELECT count(*) INTO counter FROM pg_trigger
	WHERE tgname='insert_' || the_table_name || '_trigger';
	IF(counter = 0) THEN
		the_trigger := 'CREATE TRIGGER insert_' || the_table_name || '_trigger' ||
			' BEFORE INSERT ON ' || the_table_name ||
			' FOR EACH ROW EXECUTE PROCEDURE ' || the_table_name || '_insert_trigger()';
		EXECUTE the_trigger;
	END IF;
	
	-- attach trigger to partitions
	FOR part_row IN SELECT * FROM pg_partition WHERE table_name=the_table_name
	LOOP
		the_trigger_name := the_table_name || '_' || part_row.partition_name;
		SELECT count(*) INTO counter FROM pg_trigger
		WHERE tgname='insert_' || the_trigger_name || '_trigger';
		IF(counter = 0) THEN
			the_trigger := 'CREATE TRIGGER insert_' || the_trigger_name || '_trigger' ||
				' BEFORE UPDATE ON ' || the_trigger_name ||
				' FOR EACH ROW EXECUTE PROCEDURE ' || the_table_name || '_insert_trigger()';
			EXECUTE the_trigger;
		END IF;
	END LOOP;
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_table_init(
	the_table_name TEXT
)
RETURNS VOID
AS $$
DECLARE
	counter INT;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();

	-- make sure the table exists
	IF(NOT _table_exists(the_table_name)) THEN
		RAISE EXCEPTION 'Table % does not exist.', the_table_name;
	END IF;
	
	-- make sure the table has not been partitioned already
	IF(_table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is already partitioned.', the_table_name;
	END IF;
	
	-- we can only partition a table that is empty
	EXECUTE 'SELECT COUNT(*) FROM ' || the_table_name INTO counter;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Only empty tables can be partitioned.', the_table_name;
	END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _table_exists(
	the_table_name TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
BEGIN
	SELECT COUNT(*) INTO counter FROM pg_class WHERE relname=the_table_name;
	RETURN (counter > 0);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _get_comparator_type(
	the_table_name TEXT
)
RETURNS TEXT
AS $$
BEGIN
	RETURN (
		SELECT typname::TEXT
		FROM pg_attribute
		JOIN pg_class ON attrelid=pg_class.oid
		JOIN pg_type ON atttypid=pg_type.oid
		JOIN _partition_table ON _partition_table.table_name=pg_class.relname
		WHERE pg_class.relname=the_table_name AND attname=_partition_table.expression
	);
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
	-- check some prerequisites
	PERFORM _partition_table_init(the_table_name);
	
	-- register table for partitioning
	INSERT INTO _partition_table (table_name, partition_type, expression)
	VALUES (the_table_name, 'list', the_expression);
	
	FOR the_list_value IN
		SELECT the_list_values[i] FROM generate_series(1, array_upper(the_list_values, 1)) AS i
	LOOP
		the_partition_name := substring(md5(random()::text) for 8);
		PERFORM _add_partition(the_table_name, the_partition_name, the_expression, the_list_value,
			NULL);
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
		des := des || part.expression || '=' || part.test_lower;
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
		test_lower=the_value;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Value % already exists in list partition.', the_value;
	END IF;
	
	-- create partition
	SELECT expression INTO the_expression FROM _partition_table WHERE table_name=the_table_name;
	PERFORM _add_partition(the_table_name, the_partition_name, the_expression, the_value, NULL);
	
	-- rebuild triggers
	PERFORM _rebuild_triggers(the_table_name);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_partition_by_range(
	the_table_name TEXT,
	the_expression TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
	the_partition_name TEXT;
BEGIN
	-- check some prerequisites
	PERFORM _partition_table_init(the_table_name);
	
	-- register table for partitioning
	INSERT INTO _partition_table (table_name, partition_type, expression)
	VALUES (the_table_name, 'range', the_expression);
	
	-- build and attach triggers
	PERFORM _rebuild_triggers(the_table_name);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION add_range_partition(
	the_table_name TEXT,
	the_partition_name TEXT,
	the_range_min TEXT,
	the_range_max TEXT
)
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
	the_expression TEXT;
	temp_row RECORD;
	compare_type TEXT;
BEGIN
	-- make sure partitioning feature is ready
	PERFORM _partition_init();
	
	-- get comparator
	compare_type := _get_comparator_type(the_table_name);
	
	-- make sure the table exists and is partitioned
	IF(NOT _table_is_partitioned(the_table_name)) THEN
		RAISE EXCEPTION 'Table % is not partitioned.', the_table_name;
	END IF;
	
	-- make sure this table is RANGE partitioned
	IF((SELECT partition_type FROM _partition_table WHERE table_name=the_table_name) != 'range')
	THEN
		RAISE EXCEPTION 'Table % is not partitioned by range.', the_table_name;
	END IF;
	
	-- make sure the partition does not exist
	SELECT count(*) INTO counter FROM _partition WHERE table_name=the_table_name AND
		partition_name=the_partition_name;
	IF(counter > 0) THEN
		RAISE EXCEPTION 'Partition % already exists.', the_partition_name;
	END IF;
	
	-- make sure this range value does not conflict exist
	EXECUTE 'SELECT * FROM _partition WHERE table_name=''' || the_table_name || ''' AND ' ||
		'((''' || the_range_min || '''::' || compare_type || ' BETWEEN test_lower::' ||
		compare_type || ' AND test_upper::' || compare_type || ') OR ' || '(''' || the_range_max ||
		'''::' || compare_type || ' BETWEEN test_lower::' || compare_type || ' AND test_upper::' ||
		compare_type || ')) LIMIT 1' INTO temp_row;
	IF(temp_row IS NOT NULL) THEN
		RAISE EXCEPTION 'Range conflicts with an already defined range %-%.', temp_row.test_lower,
			temp_row.test_upper;
	END IF;
	
	-- create partition
	SELECT expression INTO the_expression FROM _partition_table WHERE table_name=the_table_name;
	PERFORM _add_partition(the_table_name, the_partition_name, the_expression, the_range_min,
		the_range_max);
	
	-- rebuild triggers
	PERFORM _rebuild_triggers(the_table_name);
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION detach_partition(
	the_table_name TEXT,
	the_partition_name TEXT,
	destination_name TEXT
)
RETURNS BOOLEAN
AS $$
BEGIN
	-- make sure the destination table does not exist
	IF(_table_exists(destination_name)) THEN
		RAISE EXCEPTION 'Destination table % already exists.', destination_name;
	END IF;
	
	-- deregister partition
	DELETE FROM _partition WHERE table_name=the_table_name AND partition_name=the_partition_name;
	
	-- detach the table
	EXECUTE 'ALTER TABLE ' || the_table_name || '_' || the_partition_name || ' NO INHERIT ' ||
		the_table_name;
	EXECUTE 'ALTER TABLE ' || the_table_name || '_' || the_partition_name || ' RENAME TO ' ||
		destination_name;
		
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
