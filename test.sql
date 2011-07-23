CREATE OR REPLACE FUNCTION _partition_assert(expected BIGINT, received BIGINT)
RETURNS VOID
AS $$
BEGIN
	IF(expected != received) THEN
		RAISE EXCEPTION 'Assertion failed. Expected % but received %.',
			expected, received;
	END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_test_list()
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
BEGIN
	-- create a table
	CREATE TABLE testpart (
		parter INT,
		some_value TEXT
	);
	
	-- create partitions
	PERFORM create_table_partition_by_list('testpart', 'parter', '{1,2,3}');
	
	-- insert rows
	INSERT INTO testpart (parter, some_value) VALUES (1, 'abc');
	INSERT INTO testpart (parter, some_value) VALUES (2, 'def');
	INSERT INTO testpart (parter, some_value) VALUES (3, 'xyz');
	
	-- validate
	SELECT COUNT(*) INTO counter FROM testpart_part_1;
	PERFORM _partition_assert(1::bigint, counter::bigint);
	
	-- drop table
	PERFORM drop_partitioned_table('testpart');
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
