CREATE OR REPLACE FUNCTION _partition_assert(
	expected TEXT,
	received TEXT
)
RETURNS VOID
AS $$
BEGIN
	IF(expected != received) THEN
		RAISE EXCEPTION 'Assertion failed. Expected % but received %.',
			expected, received;
	END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_test()
RETURNS SETOF TEXT
AS $$
BEGIN
	RETURN NEXT 'List 1: ' || _partition_test_list1();
	RETURN NEXT 'List 2: ' || _partition_test_list2();
	RETURN NEXT 'Range 1: ' || _partition_test_range1();
	RETURN;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_test_list1()
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
	EXECUTE 'SELECT COUNT(*) FROM testpart_' ||
		(SELECT partition_name FROM pg_partition WHERE table_name='testpart' AND test_lower='1')
		INTO counter;
	PERFORM _partition_assert(1::text, counter::text);
	
	-- add a partition
	PERFORM add_list_partition('testpart', 'extra', '4');
	
	-- insert row into newly created partition
	INSERT INTO testpart (parter, some_value) VALUES (4, 'qwerty');
	INSERT INTO testpart (parter, some_value) VALUES (4, 'abcde');
	
	-- validate
	SELECT COUNT(*) INTO counter FROM testpart_extra;
	PERFORM _partition_assert(2::text, counter::text);
	
	-- move a row to a different partition
	UPDATE testpart SET parter=1, some_value='works' WHERE some_value='def';
	
	-- validate
	EXECUTE 'SELECT COUNT(*) FROM testpart_' ||
		(SELECT partition_name FROM pg_partition WHERE table_name='testpart' AND test_lower='1')
		INTO counter;
	PERFORM _partition_assert(2::text, counter::text);
	
	EXECUTE 'SELECT COUNT(*) FROM testpart WHERE some_value=''works''' INTO counter;
	PERFORM _partition_assert(1::text, counter::text);
	
	-- drop table
	PERFORM drop_partitioned_table('testpart');
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_test_list2()
RETURNS BOOLEAN
AS $$
DECLARE
	counter INT;
BEGIN
	-- create a table
	CREATE TABLE testpart (
		parter TEXT,
		some_value TEXT
	);
	
	-- create partitions
	PERFORM create_table_partition_by_list('testpart', 'parter', '{a,b,c d}');
	
	-- insert rows
	INSERT INTO testpart (parter, some_value) VALUES ('a', 'abc');
	INSERT INTO testpart (parter, some_value) VALUES ('b', 'def');
	INSERT INTO testpart (parter, some_value) VALUES ('c d', 'xyz');
	
	-- validate
	EXECUTE 'SELECT COUNT(*) FROM testpart_' ||
		(SELECT partition_name FROM pg_partition WHERE table_name='testpart' AND test_lower='c d')
		INTO counter;
	PERFORM _partition_assert(1::text, counter::text);
	
	-- drop table
	PERFORM drop_partitioned_table('testpart');
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_test_range1()
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
	PERFORM create_table_partition_by_range('testpart', 'parter');
	PERFORM add_range_partition('testpart', 'range1', '1', '3');
	PERFORM add_range_partition('testpart', 'range2', '4', '6');
	
	-- insert rows
	INSERT INTO testpart (parter, some_value) VALUES (2, 'abc');
	INSERT INTO testpart (parter, some_value) VALUES (5, 'def');
	INSERT INTO testpart (parter, some_value) VALUES (6, 'xyz');
	
	-- validate
	SELECT COUNT(*) INTO counter FROM testpart_range2;
	PERFORM _partition_assert(2::text, counter::text);
	
	-- add a partition
	PERFORM add_range_partition('testpart', 'range3', '15', '20');
	
	-- insert row into newly created partition
	INSERT INTO testpart (parter, some_value) VALUES (17, 'qwerty');
	
	-- validate
	SELECT COUNT(*) INTO counter FROM testpart_range3;
	PERFORM _partition_assert(1::text, counter::text);
	
	-- move a row to a different partition
	UPDATE testpart SET parter=1, some_value='works' WHERE some_value='xyz';
	
	-- validate
	SELECT COUNT(*) INTO counter FROM testpart_range1;
	PERFORM _partition_assert(2::text, counter::text);
	
	SELECT COUNT(*) INTO counter FROM testpart WHERE some_value='works';
	PERFORM _partition_assert(1::text, counter::text);
	
	-- drop table
	PERFORM drop_partitioned_table('testpart');
	
	-- success
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
