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
	CREATE VIEW pg_partitions AS
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
	the_trigger TEXT;
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
	
	-- add partitions and build trigger
	the_trigger := 'CREATE OR REPLACE FUNCTION ' || the_table_name || '_insert_trigger() ' ||
		'RETURNS TRIGGER AS $TRIGGER$ BEGIN IF(FALSE) THEN /* do nothing */ ';
	
	FOR the_list_value IN
		SELECT the_list_values[i] FROM generate_series(1, array_upper(the_list_values, 1)) AS i
	LOOP
		the_partition_name := 'part_' || the_list_value;
		
		-- register partition
		INSERT INTO _partition (table_name, partition_name, test)
		VALUES (the_table_name, the_partition_name, the_list_value);
		
		-- append trigger
		the_trigger := the_trigger || ' ELSIF (NEW.' || the_expression || ' = ' ||
			the_list_value || ') THEN INSERT INTO ' || the_table_name || '_' ||
			the_partition_name || ' VALUES (NEW.*); ';
	
		-- create partition tables
		EXECUTE 'CREATE TABLE ' || the_table_name || '_' || the_partition_name || '( CHECK((' ||
			the_expression || ') = (' || the_list_value || ')) ) INHERITS (' || the_table_name ||
			')';
	END LOOP;
	
	-- create trigger
	the_trigger := the_trigger || 'ELSE RAISE EXCEPTION ''List value out of range.''; END IF;' ||
		' RETURN NULL; END; $TRIGGER$ LANGUAGE plpgsql';
	EXECUTE the_trigger;
	
	-- attach trigger
	the_trigger := 'CREATE TRIGGER insert_' || the_table_name || '_trigger' ||
		' BEFORE INSERT ON ' || the_table_name ||
		' FOR EACH ROW EXECUTE PROCEDURE ' || the_table_name || '_insert_trigger()';
	EXECUTE the_trigger;
	
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
