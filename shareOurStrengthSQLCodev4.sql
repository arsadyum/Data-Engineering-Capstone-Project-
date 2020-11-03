/*************************************************
WISCONSIN TEST
**************************************************/

/******** 
Load in Data Dictionary Files
*********/
create table #data_dictionary_template1(raw_data_column varchar(max),raw_data_column_name varchar(max), equivalent_clean_data_name varchar(max), notes varchar(max));
bulk insert #data_dictionary_template1
from 'C:\Users\rahma\OneDrive\Desktop\Dean Data\data_dictionary_template_1.txt'
with (FirstRow = 2, FieldTerminator = '\t', RowTerminator = '\n');

create table #data_dictionary_template2(raw_data_column varchar(max),raw_data_column_name varchar(max), equivalent_clean_data_name varchar(max), notes varchar(max));
bulk insert #data_dictionary_template2
from 'C:\Users\rahma\OneDrive\Desktop\Dean Data\data_dictionary_template_2.txt'
with (FirstRow = 2, FieldTerminator = '\t', RowTerminator = '\n');
--SELECT * FROM #data_dictionary_template1

/********
Dynamically Load in CSV File 2017_2018_SBP_MEAL_PARTICIPATION

EDIT:NEEDS TO BE TAB DELIMITED
*********/

create table #tmpColumns(columnNames varchar(max));
bulk insert #tmpColumns
from 'C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_SBP_MEAL_PARTICIPATION.txt'
with (FirstRow = 1, LastRow = 1, FieldTerminator = '\t', RowTerminator = '\n');

/*****
Clean up the load for columns

replace tabs with commas for column names to be used in the dynamic table creation
*****/
SELECT REPLACE(columnNames,'n++','') as columnNames INTO #tmpColumns2 FROM #tmpColumns

SELECT  CONCAT('"',REPLACE(columnNames, char(9), '","'),'"') as columnNames INTO #tmpColumns3 From #tmpColumns2

/*****
The following loop dynamically creates a table based on the column names from above

For standardization purposes all columns are varchar(1000) and should be changed later in the script

SELECT * FROM #tmpColumns3

SELECT * FROM  ##tmpBulk
*****/

declare @sql as varchar(max)
select @sql = 'create table ##tmpBulk (' + replace(columnNames,',',' varchar(max),') + ' varchar(max));
				bulk insert ##tmpBulk
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_SBP_MEAL_PARTICIPATION.txt''
				with (FirstRow = 2, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				
				create table ##tmpBulkColumns (' + replace(columnNames,',',' varchar(max),') + ' varchar(max));
				bulk insert ##tmpBulkColumns
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_SBP_MEAL_PARTICIPATION.txt''
				with (FirstRow = 1, LastRow = 1, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				'
from #tmpColumns3

exec(@sql)    

--Drop column tables
drop table #tmpColumns
drop table #tmpColumns2

--select * from ##tmpBulk WHERE school_name like '%,%'
--SELECT * FROM ##tmpBulkColumns

/*****
Put column names into rows

this is done to be able to join to the dictionaries
*****/

SELECT * INTO dbo.tmp_vg_column_name FROM ##tmpBulkColumns

IF OBJECT_ID('tempdb.dbo.#columnNames ', 'U') IS NOT NULL
  DROP TABLE #columnNames; 

SELECT name INTO #columnNames FROM sys.columns WHERE object_Id = object_id('dbo.tmp_vg_column_name')

DROP TABLE dbo.tmp_vg_column_name
--SELECT * FROM #columnNames


/********
FOR FILE 1


Column Label match based on data dictionary

This pulls in clean label names and rules for columns where applicable
*********/
drop table #clean_data_lookup 
SELECT DISTINCT name as raw_data_name,
	CASE WHEN c.equivalent_clean_data_name IS NULL THEN b.equivalent_clean_data_name
		ELSE c.equivalent_clean_data_name END as equivalent_clean_data_name,
		CASE WHEN b.notes IS  NULL THEN c.notes
		ELSE c.notes END as notes
INTO #clean_data_lookup 
FROM #columnNames a
LEFT JOIN #data_dictionary_template1 b ON a.name = b.raw_data_column_name
LEFT JOIN #data_dictionary_template2 c ON a.name = c.raw_data_column_name


/*
SELECT * FROM #data_dictionary_template1
SELECT * FROM #data_dictionary_template2
SELECT * FROM #clean_data_lookup WHERE equivalent_clean_data_name IS NULL
select * from ##tmpBulk
*/


/****
DROPPING COLUMNS NOT IN THE DICTIONARY

Create lookup for unneeded columns
*****/


--DROP TABLE #deletecolumns
SELECT raw_data_name, ROW_NUMBER() OVER (ORDER by raw_data_name) as row_num
INTO #deletecolumns 
FROM #clean_data_lookup 
WHERE equivalent_clean_data_name IS NULL
OR equivalent_clean_data_name like '%NOT USED%'

--SELECT * FROM #deletecolumns

/*****
Loop through the delete columns and remove them dynamically and individually
*****/
declare @counta INT;
SET @counta = 1;
declare @max_rowa INT;

SELECT @max_rowa = MAX(row_num) FROM #deletecolumns;

WHILE @counta <= @max_rowa
BEGIN
declare @columnName varchar(1000);
SELECT @columnName = raw_data_name FROM #deletecolumns WHERE row_num = @counta;

declare @sql2 as varchar(max);
SET @sql2 = N'ALTER TABLE ##tmpBulk DROP COLUMN '+@columnName+'';

EXECUTE(@sql2);

set @counta = @counta+1;
END;


/***
put in a unique key for raw table

This is done to be able to stitch columns back together accordingly
***/

SELECT ROW_NUMBER() OVER (ORDER BY AGENCY_CODE) as row_num, * INTO ##tmpBulk2 from ##tmpBulk;


--SELECT * FROM ##tmpBulk2

/***
pull 1:1 mappings
***/
--DROP TABLE #column_renames
SELECT equivalent_clean_data_name, ROW_NUMBER() OVER (ORDER BY equivalent_clean_data_name) as row_num
INTO #column_renames
FROM
(SELECT equivalent_clean_data_name, COUNT(*) as count
FROM #clean_data_lookup 
GROUP BY equivalent_clean_data_name) a
WHERE count = 1 AND equivalent_clean_data_name NOT LIKE '%NOT USED%'

--SELECT * FROM #column_renames

--SELECT * FROM #clean_data_lookup

/*****
Creating lookup with unique identifiers for old column names and new column names
*****/

SELECT a.row_num,b.raw_data_name, a.equivalent_clean_data_name 
INTO #column_renames2
FROM #column_renames a
JOIN #clean_data_lookup b ON a.equivalent_clean_data_name = b.equivalent_clean_data_name

--SELECT * FROM #column_renames2

--SELECT * FROM  ##tmpBulk WHERE AGENCY_NAME like '%Seeds of Health%'

/*****
Loop through the columns that need to be renamed and rename them

This is done by breaking up the table and changing each column individually
*****/
declare @count INT;
SET @count = 1;
declare @max_row INT;

SELECT @max_row = MAX(row_num) FROM #column_renames;

WHILE @count <= @max_row
BEGIN

declare @rawcolumnName varchar(1000);
SELECT @rawcolumnName = raw_data_name FROM #column_renames2 WHERE row_num = @count;
declare @newcolumnName varchar(1000);
SELECT @newcolumnName = equivalent_clean_data_name FROM #column_renames2 WHERE row_num = @count;
declare @count_string varchar(1000);
SELECT @count_string = cast(@count as varchar);
declare @count_string_prev varchar(1000);
SELECT @count_string_prev = cast(@count-1 as varchar);


declare @sql3 as varchar(max);
SET @sql3 = N'SELECT row_num, "'+@rawcolumnName+'" as "'+@newcolumnName+'"
INTO ##tmp_rename_'+@count_string+' FROM ##tmpBulk2


IF '+@count_string+'=2
IF OBJECT_ID(''tempdb..##tmp_renamed_housed_'+@count_string+''') IS  NULL
	SELECT a.*,b."'+@newcolumnName+'"
	INTO ##tmp_renamed_housed_'+@count_string+'
	FROM ##tmp_rename_'+@count_string_prev+' a
	JOIN ##tmp_rename_'+@count_string+' b ON a.row_num = b.row_num




';

EXECUTE(@sql3);


--	INTO ##tmp_renamed_housed_'+@count_string+'

/*****
Housing table with conditionals needed to be in seperate executions

This was done because an exist error was occuring
*****/

declare @sql4 as varchar(max);
SET @sql4 = N'
	IF '+@count_string+'>2
		SELECT a.*,b."'+@newcolumnName+'"
		INTO ##tmp_renamed_housed_'+@count_string+'
		FROM ##tmp_renamed_housed_'+@count_string_prev+' a
		JOIN ##tmp_rename_'+@count_string+' b ON a.row_num = b.row_num

'
EXECUTE(@sql4);

set @count = @count+1;
END;

declare @sql5 as varchar(max);
SET @sql5 = N'
	SELECT *
	INTO ##tmp_renamed_housed_final
	FROM ##tmp_renamed_housed_'+@count_string+'
';

EXECUTE(@sql5);
/********
Dynamically Load in CSV File 2017_2018_NSLP_MEAL_PARTICIPATION

EDIT:NEEDS TO BE TAB DELIMITED
*********/

--DROP TABLE #tmpColumns_file2
create table #tmpColumns_file2(columnNames varchar(max));
bulk insert #tmpColumns_file2
from 'C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_NSLP_MEAL_PARTICIPATION.txt'
with (FirstRow = 1, LastRow = 1, FieldTerminator = '\t', RowTerminator = '\n');

/*****
Clean up the load for columns

replace tabs with commas for column names to be used in the dynamic table creation
*****/
SELECT REPLACE(columnNames,'n++','') as columnNames INTO #tmpColumns2_file2 FROM #tmpColumns_file2

SELECT CONCAT('"',REPLACE(columnNames, char(9), '","'),'"') as columnNames INTO #tmpColumns3_file2 From #tmpColumns2_file2

/*****
The following loop dynamically creates a table based on the column names from above

For standardization purposes all columns are varchar(1000) and should be changed later in the script

SELECT * FROM #tmpColumns3_file2
*****/

declare @sql_file2 as varchar(max)
select @sql_file2 = 'create table ##tmpBulk_file2 (' + replace(columnNames,',',' varchar(1000),') + ' varchar(1000));
				bulk insert ##tmpBulk_file2
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_NSLP_MEAL_PARTICIPATION.txt''
				with (FirstRow = 2, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				
				create table ##tmpBulkColumns_file2 (' + replace(columnNames,',',' varchar(1000),') + ' varchar(1000));
				bulk insert ##tmpBulkColumns_file2
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\2017_2018_NSLP_MEAL_PARTICIPATION.txt''
				with (FirstRow = 1, LastRow = 1, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				'
from #tmpColumns3_file2

exec(@sql_file2)    

--Drop column tables
drop table #tmpColumns_file2
drop table #tmpColumns2_file2

--select * from ##tmpBulk_file2 WHERE school_name like '%,%'
--SELECT * FROM ##tmpBulkColumns

/*****
Put column names into rows

this is done to be able to join to the dictionaries
*****/

SELECT * INTO dbo.tmp_vg_column_name_file2 FROM ##tmpBulkColumns_file2

IF OBJECT_ID('tempdb.dbo.#columnNames_file2', 'U') IS NOT NULL
  DROP TABLE #columnNames_file2; 

SELECT name INTO #columnNames_file2 FROM sys.columns WHERE object_Id = object_id('dbo.tmp_vg_column_name_file2')

DROP TABLE dbo.tmp_vg_column_name_file2

/********
FOR FILE 2

Column Label match based on data dictionary

This pulls in clean label names and rules for columns where applicable
*********/

SELECT DISTINCT name as raw_data_name,
	CASE WHEN b.equivalent_clean_data_name IS NULL THEN c.equivalent_clean_data_name
		ELSE b.equivalent_clean_data_name END as equivalent_clean_data_name,
		CASE WHEN b.notes IS  NULL THEN c.notes
		ELSE b.notes END as notes
INTO #clean_data_lookup_file2 
FROM #columnNames_file2 a
LEFT JOIN #data_dictionary_template1 b ON a.name = b.raw_data_column_name
LEFT JOIN #data_dictionary_template2 c ON a.name = c.raw_data_column_name


/*
SELECT * FROM #data_dictionary_template1
SELECT * FROM #data_dictionary_template2
SELECT * FROM #clean_data_lookup WHERE equivalent_clean_data_name IS NULL
select * from ##tmpBulk
*/


/****
DROPPING COLUMNS NOT IN THE DICTIONARY

Create lookup for unneeded columns
*****/


--DROP TABLE #deletecolumns
SELECT raw_data_name, ROW_NUMBER() OVER (ORDER by raw_data_name) as row_num
INTO #deletecolumns_file2 
FROM #clean_data_lookup_file2 
WHERE equivalent_clean_data_name IS NULL
OR equivalent_clean_data_name like '%NOT USED%'

--SELECT * FROM #deletecolumns

/*****
Loop through the delete columns and remove them dynamically and individually
*****/
declare @counta_file2 INT;
SET @counta_file2 = 1;
declare @max_rowa_file2 INT;

SELECT @max_rowa_file2 = MAX(row_num) FROM #deletecolumns_file2;

WHILE @counta_file2 <= @max_rowa_file2
BEGIN
declare @columnName_file2 varchar(1000);
SELECT @columnName_file2 = raw_data_name FROM #deletecolumns_file2 WHERE row_num = @counta_file2;

declare @sql2_file2 as varchar(max);
SET @sql2_file2 = N'ALTER TABLE ##tmpBulk_file2 DROP COLUMN '+@columnName_file2+'';

EXECUTE(@sql2_file2);

set @counta_file2 = @counta_file2+1;
END;



/***
put in a unique key for raw table

This is done to be able to stitch columns back together accordingly
***/

SELECT ROW_NUMBER() OVER (ORDER BY AGENCY_CODE) as row_num, * INTO ##tmpBulk2_file2 from ##tmpBulk_file2;


--SELECT * FROM ##tmpBulk2

/***
pull 1:1 mappings
***/
--DROP TABLE #column_renames
SELECT equivalent_clean_data_name, ROW_NUMBER() OVER (ORDER BY equivalent_clean_data_name) as row_num
INTO #column_renames_file2
FROM
(SELECT equivalent_clean_data_name, COUNT(*) as count
FROM #clean_data_lookup_file2 
GROUP BY equivalent_clean_data_name) a
WHERE count = 1 AND equivalent_clean_data_name NOT LIKE '%NOT USED%'

--SELECT * FROM #column_renames

--SELECT * FROM #clean_data_lookup

/*****
Creating lookup with unique identifiers for old column names and new column names
*****/

SELECT a.row_num,b.raw_data_name, a.equivalent_clean_data_name 
INTO #column_renames2_file2
FROM #column_renames_file2 a
JOIN #clean_data_lookup_file2 b ON a.equivalent_clean_data_name = b.equivalent_clean_data_name

--SELECT * FROM #column_renames2

--SELECT * FROM  ##tmpBulk WHERE AGENCY_NAME like '%Seeds of Health%'

/*****
Loop through the columns that need to be renamed and rename them

This is done by breaking up the table and changing each column individually
*****/
declare @count_file2 INT;
SET @count_file2 = 1;
declare @max_row_file2 INT;

SELECT @max_row_file2 = MAX(row_num) FROM #column_renames_file2;

WHILE @count_file2 <= @max_row_file2
BEGIN

declare @rawcolumnName_file2 varchar(1000);
SELECT @rawcolumnName_file2 = raw_data_name FROM #column_renames2_file2 WHERE row_num = @count_file2;
declare @newcolumnName_file2 varchar(1000);
SELECT @newcolumnName_file2 = equivalent_clean_data_name FROM #column_renames2_file2 WHERE row_num = @count_file2;
declare @count_string_file2 varchar(1000);
SELECT @count_string_file2 = cast(@count_file2 as varchar);
declare @count_string_prev_file2 varchar(1000);
SELECT @count_string_prev_file2 = cast(@count_file2-1 as varchar);


declare @sql3_file2 as varchar(max);
SET @sql3_file2 = N'SELECT row_num, "'+@rawcolumnName_file2+'" as "'+@newcolumnName_file2+'"
INTO ##tmp_rename_'+@count_string_file2+'_file2 FROM ##tmpBulk2_file2


IF '+@count_string_file2+'=2
IF OBJECT_ID(''tempdb..##tmp_renamed_housed_'+@count_string_file2+'_file2'') IS  NULL
	SELECT a.*,b."'+@newcolumnName_file2+'"
	INTO ##tmp_renamed_housed_'+@count_string_file2+'_file2
	FROM ##tmp_rename_'+@count_string_prev_file2+'_file2 a
	JOIN ##tmp_rename_'+@count_string_file2+'_file2 b ON a.row_num = b.row_num




';

EXECUTE(@sql3_file2);
--	INTO ##tmp_renamed_housed_'+@count_string+'

/*****
Housing table with conditionals needed to be in seperate executions

This was done because an exist error was occuring
*****/

declare @sql4_file2 as varchar(max);
SET @sql4_file2 = N'
	IF '+@count_string_file2+'>2
		SELECT a.*,b."'+@newcolumnName_file2+'"
		INTO ##tmp_renamed_housed_'+@count_string_file2+'_file2
		FROM ##tmp_renamed_housed_'+@count_string_prev_file2+'_file2 a
		JOIN ##tmp_rename_'+@count_string_file2+'_file2 b ON a.row_num = b.row_num

'
EXECUTE(@sql4_file2);

set @count_file2 = @count_file2+1;
END;

declare @sql5_file2 as varchar(max);
SET @sql5_file2 = N'
	SELECT *
	INTO ##tmp_renamed_housed_final_file2
	FROM ##tmp_renamed_housed_'+@count_string_file2+'_file2
';

EXECUTE(@sql5_file2);

/********
Dynamically Load in CSV File 2017_2018_SBP_MEAL_PARTICIPATION

EDIT:NEEDS TO BE TAB DELIMITED
*********/

--DROP TABLE #tmpColumns_file3
create table #tmpColumns_file3(columnNames varchar(max));
bulk insert #tmpColumns_file3
from 'C:\Users\rahma\OneDrive\Desktop\Dean Data\WI_NCES_School_Data_SY15-16.txt'
with (FirstRow = 1, LastRow = 1, FieldTerminator = '\t', RowTerminator = '\n');

/*****
Clean up the load for columns

replace tabs with commas for column names to be used in the dynamic table creation
*****/
SELECT REPLACE(columnNames,'n++','') as columnNames INTO #tmpColumns2_file3 FROM #tmpColumns_file3

SELECT REPLACE(columnNames,'"','') as columnNames INTO #tmpColumns2_file3_updated FROM #tmpColumns2_file3

SELECT CONCAT('"',REPLACE(columnNames, char(9), '","'),'"') as columnNames INTO #tmpColumns3_file3 From #tmpColumns2_file3_updated

/*****
The following loop dynamically creates a table based on the column names from above

For standardization purposes all columns are varchar(1000) and should be changed later in the script

SELECT * FROM #tmpColumns2_file3
SELECT * FROM #tmpColumns3_file3

SELECT * FROM ##tmpBulk_file3
*****/

declare @sql_file3 as varchar(max)
select @sql_file3 = 'create table ##tmpBulk_file3 (' + replace(columnNames,',',' varchar(1000),') + ' varchar(1000));
				bulk insert ##tmpBulk_file3
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\WI_NCES_School_Data_SY15-16.txt''
				with (FirstRow = 2, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				
				create table ##tmpBulkColumns_file3 (' + replace(columnNames,',',' varchar(1000),') + ' varchar(1000));
				bulk insert ##tmpBulkColumns_file3
				from ''C:\Users\rahma\OneDrive\Desktop\Dean Data\WI_NCES_School_Data_SY15-16.txt''
				with (FirstRow = 1, LastRow = 1, FieldTerminator = ''\t'', RowTerminator = ''\n'');
				
				'
from #tmpColumns3_file3

--print @sql_file3
exec(@sql_file3)    

--Drop column tables
drop table #tmpColumns_file3
drop table #tmpColumns2_file3

/*****
Put column names into rows

this is done to be able to join to the dictionaries
*****/

SELECT * INTO dbo.tmp_vg_column_name_file3 FROM ##tmpBulkColumns_file3

IF OBJECT_ID('tempdb.dbo.#columnNames_file3 ', 'U') IS NOT NULL
  DROP TABLE #columnNames_file3; 

SELECT name INTO #columnNames_file3 FROM sys.columns WHERE object_Id = object_id('dbo.tmp_vg_column_name_file3')

DROP TABLE dbo.tmp_vg_column_name_file3
--SELECT * FROM #columnNames
/********
FOR FILE 3

Column Label match based on data dictionary

This pulls in clean label names and rules for columns where applicable
*********/

SELECT DISTINCT name as raw_data_name,
	CASE WHEN b.equivalent_clean_data_name IS NULL THEN c.equivalent_clean_data_name
		ELSE b.equivalent_clean_data_name END as equivalent_clean_data_name,
		CASE WHEN b.notes IS  NULL THEN c.notes
		ELSE b.notes END as notes
INTO #clean_data_lookup_file3 
FROM #columnNames_file3 a
LEFT JOIN #data_dictionary_template1 b ON a.name = b.raw_data_column_name
LEFT JOIN #data_dictionary_template2 c ON a.name = c.raw_data_column_name


/*
SELECT * FROM #data_dictionary_template1
SELECT * FROM #data_dictionary_template2
SELECT * FROM #clean_data_lookup WHERE equivalent_clean_data_name IS NULL
select * from ##tmpBulk
*/


/****
DROPPING COLUMNS NOT IN THE DICTIONARY

Create lookup for unneeded columns
*****/


--DROP TABLE #deletecolumns
SELECT raw_data_name, ROW_NUMBER() OVER (ORDER by raw_data_name) as row_num
INTO #deletecolumns_file3 
FROM #clean_data_lookup_file3 
WHERE equivalent_clean_data_name IS NULL
OR equivalent_clean_data_name like '%NOT USED%'

--SELECT * FROM #deletecolumns
--SELECT * FROM #deletecolumns_file3
--SELECT * FROM #clean_data_lookup_file3

/*****
Loop through the delete columns and remove them dynamically and individually
*****/
declare @counta_file3 INT;
SET @counta_file3 = 1;
declare @max_rowa_file3 INT;

SELECT @max_rowa_file3 = MAX(row_num) FROM #deletecolumns_file3;

WHILE @counta_file3 <= @max_rowa_file3
BEGIN
declare @columnName_file3 varchar(1000);
SELECT @columnName_file3 = raw_data_name FROM #deletecolumns_file3 WHERE row_num = @counta_file3;

declare @sql2_file3 as varchar(max);
SET @sql2_file3 = N'ALTER TABLE ##tmpBulk_file3 DROP COLUMN "'+@columnName_file3+'"';

--print @sql2_file3
--print @max_rowa_file3
--EXECUTE(@sql2_file3);

set @counta_file3 = @counta_file3+1;
END;


/***
put in a unique key for raw table

This is done to be able to stitch columns back together accordingly
***/

SELECT ROW_NUMBER() OVER (ORDER BY "School Name") as row_num, * INTO ##tmpBulk2_file3 from ##tmpBulk_file3;


--SELECT * FROM ##tmpBulk_file3

/***
pull 1:1 mappings
***/
--DROP TABLE #column_renames
SELECT equivalent_clean_data_name, ROW_NUMBER() OVER (ORDER BY equivalent_clean_data_name) as row_num
INTO #column_renames_file3
FROM
(SELECT equivalent_clean_data_name, COUNT(*) as count
FROM #clean_data_lookup_file3 
GROUP BY equivalent_clean_data_name) a
WHERE count = 1 AND equivalent_clean_data_name NOT LIKE '%NOT USED%'

--SELECT * FROM #column_renames

--SELECT * FROM #clean_data_lookup

/*****
Creating lookup with unique identifiers for old column names and new column names
*****/

SELECT a.row_num,b.raw_data_name, a.equivalent_clean_data_name 
INTO #column_renames2_file3
FROM #column_renames_file3 a
JOIN #clean_data_lookup_file3 b ON a.equivalent_clean_data_name = b.equivalent_clean_data_name

--SELECT * FROM #column_renames2

--SELECT * FROM  ##tmpBulk WHERE AGENCY_NAME like '%Seeds of Health%'

/*****
Loop through the columns that need to be renamed and rename them

This is done by breaking up the table and changing each column individually
*****/
declare @count_file3 INT;
SET @count_file3 = 1;
declare @max_row_file3 INT;

SELECT @max_row_file3 = MAX(row_num) FROM #column_renames_file3;

WHILE @count_file3 <= @max_row_file3
BEGIN

declare @rawcolumnName_file3 varchar(1000);
SELECT @rawcolumnName_file3 = raw_data_name FROM #column_renames2_file3 WHERE row_num = @count_file3;
declare @newcolumnName_file3 varchar(1000);
SELECT @newcolumnName_file3 = equivalent_clean_data_name FROM #column_renames2_file3 WHERE row_num = @count_file3;
declare @count_string_file3 varchar(1000);
SELECT @count_string_file3 = cast(@count_file3 as varchar);
declare @count_string_prev_file3 varchar(1000);
SELECT @count_string_prev_file3 = cast(@count_file3-1 as varchar);

--SELECT * FROM ##tmp_rename_1_file3

declare @sql3_file3 as varchar(max);
SET @sql3_file3 = N'SELECT row_num, "'+@rawcolumnName_file3+'" as "'+@newcolumnName_file3+'"
INTO ##tmp_rename_'+@count_string_file3+'_file3 FROM ##tmpBulk2_file3


IF '+@count_string_file3+'=2
IF OBJECT_ID(''tempdb..##tmp_renamed_housed_'+@count_string_file3+'_file3'') IS  NULL
	SELECT a.*,b."'+@newcolumnName_file3+'"
	INTO ##tmp_renamed_housed_'+@count_string_file3+'_file3
	FROM ##tmp_rename_'+@count_string_prev_file3+'_file3 a
	JOIN ##tmp_rename_'+@count_string_file3+'_file3 b ON a.row_num = b.row_num




';

EXECUTE(@sql3_file3);


--	INTO ##tmp_renamed_housed_'+@count_string+'

/*****
Housing table with conditionals needed to be in seperate executions

This was done because an exist error was occuring
*****/

declare @sql4_file3 as varchar(max);
SET @sql4_file3 = N'
	IF '+@count_string_file3+'>2
		SELECT a.*,b."'+@newcolumnName_file3+'"
		INTO ##tmp_renamed_housed_'+@count_string_file3+'_file3
		FROM ##tmp_renamed_housed_'+@count_string_prev_file3+'_file3 a
		JOIN ##tmp_rename_'+@count_string_file3+'_file3 b ON a.row_num = b.row_num

'
EXECUTE(@sql4_file3);

set @count_file3 = @count_file3+1;
END;

declare @sql5_file3 as varchar(max);
SET @sql5_file3 = N'
	SELECT *
	INTO ##tmp_renamed_housed_final_file3
	FROM ##tmp_renamed_housed_'+@count_string_file3+'_file3
';

EXECUTE(@sql5_file3);



--SELECT * FROM ##tmp_renamed_housed_1_file3
--SELECT * FROM ##tmp_rename_4

/*


SELECT * FROM ##tmp_renamed_housed_final
SELECT * FROM ##tmp_renamed_housed_final_file2
SELECT * FROM ##tmp_renamed_housed_final_file3

SELECT * FROM ##tmp_rename_1_file3

*/


/********
Add Derived Columns
*********/

--School Type Original


SELECT row_num,CASE WHEN "PUBLIC" = 'YES' AND "SCHOOL TYPE" != 'RCCI'
	THEN 'Public'
	WHEN "PUBLIC" = 'NO' AND "SCHOOL TYPE" != 'RCCI'
	THEN 'Nonpublic'
	WHEN "PUBLIC" = 'YES' AND "SCHOOL TYPE" = 'RCCI' 
	THEN 'Public RCCI'
	WHEN "PUBLIC" = 'NO' AND "SCHOOL TYPE" = 'RCCI' 
	THEN 'Nonpublic RCCI'
	ELSE NULL
	END as "School Type-Original"
	INTO #tmp_test
FROM ##tmpBulk2_file2


--Breakfast Delivery Model from State Agency-Original

--DROP TABLE #tmp_test2
SELECT row_num,CONCAT('O=', TRADITIONAL_MODEL, 
				', P=', MID_MORNING_MODEL, 
				', Q=', CLASSROOM_MODEL, 
				', R=', REDUCED_PRICE_MODEL, 
				', S=', GRAB_N_GO_MODEL,
				', T=', FREE_MODEL)
	 as "Breakfast Delivery Model from State Agency-Original"
	 INTO #tmp_test2
FROM ##tmpBulk2

-- SELECT * FROM #tmp_test2
-- SELECT DISTINCT("Breakfast Delivery Model from State Agency-Original") from #tmp_test2

SELECT a.*, b."School Type-Original"
INTO #join1
FROM ##tmp_renamed_housed_final_file2 a
JOIN #tmp_test b ON a.row_num = b.row_num


SELECT a.*, b."Breakfast Delivery Model from State Agency-Original"
INTO #join2
FROM ##tmp_renamed_housed_final a
JOIN #tmp_test2 b ON a.row_num = b.row_num

/****
Join tables from above into one table
pad the district id for an extra precaution
******/

SELECT a.*,b.* 

FROM #join1 a
JOIN #join2 b ON a."school name" = b."school name" AND a."claim date" = b."claim date" 
				AND CASE WHEN len(a."district id") = 6 THEN a."district id" 
				ELSE  RIGHT ('000000'+a."district id",6) END 
	
				= 
	  
				CASE WHEN len(b."district id") = 6 THEN b."district id" 
				ELSE  RIGHT ('000000'+b."district id",6) END 

select * from #join1
/********
Update datatypes (if necessary)
*********/

/********
Formulas/rules applied to create derived columns
Deduplication, aggregation
*********/
/*ADD FR Lunch Meals([Lunch Meals-Free]+[Lunch Meals-Reduced]{if both null then null}, if any of the column is null then use [Lunch Meals-Free and Reduced]) 
*/
-- drop table ##tmp_lunch 
select * from #join1 --Lunch
select * from #join2 --Brekfast

SELECT row_num, "Operating Days-Lunch Only", CAST("Lunch Meals-Free" as float) + CAST("Lunch Meals-Reduced" as float) as "FR Lunch Meals" --what if Lunch Meals-Free or Lunch Meals-Reduced columns missing? 
INTO #tmp_lunch
FROM #join1

-- select * from #tmp_lunch

/**ADD FR Lunch ADP(IF [FR Lunch Meals]) THEN NULL,,, IF [Operating Days-Lunch Only]) AND [Operating Days] is NULL THEN NULL,,, 
IF [Operating Days-Lunch Only] is NULL AND [Operating Days] is NOT NULL then   [FR Lunch Meals] /[Operating Days] or [FR Lunch Meals]/[Operating Days-Lunch Only])
when Operating Days-Lunch Only is Missing USE [Operating Days]
**/
-- drop ##tmp_lunch1

SELECT row_num, "FR Lunch Meals", CAST("FR Lunch Meals" as float) / CAST("Operating Days-Lunch Only" as float) as "FR Lunch ADP"
INTO ##tmp_lunch1
FROM #tmp_lunch

-- select * from #tmp_lunch
-- SELECT * FROM ##tmp_lunch1 (all lunch calculations) 
/*
ADD FR Breakfast Meals(IF [Breakfast Meals-Free] AND [Breakfast Meals-Reduced]) is NULL then USE Breakfast Meals-Free and Reduced otherwise [Breakfast Meals-Free] + [Breakfast Meals-Reduced]
*/
SELECT row_num, "Operating Days-Breakfast Only", CAST("Breakfast Meals-Free" as float) + CAST("Breakfast Meals-Reduced" as float) as "FR Breakfast Meals" 
INTO #tmp_brf
FROM #join2

-- SELECT * FROM #tmp_brf
/*
ADD FR Breakfast ADP(IF [FR Breakfast Meals]IS NULL THEN NULL,,, IF [Operating Days-Breakfast Only] AND [Operating Days] IS NULL THEN NULL,,,
IF [Operating Days-Breakfast Only] IS NULL AND [Operating Days] IS NOT NULL THEN [FR Breakfast Meals] /[Operating Days],,, or [FR Breakfast Meals]/[Operating Days-Breakfast Only]
*/

SELECT row_num, "FR Breakfast Meals", CAST("FR Breakfast Meals" as float) / CAST("Operating Days-Breakfast Only" as float) as "FR Breakfast ADP"
INTO ##tmp_brf1
FROM #tmp_brf

-- SELECT * FROM ##tmp_brf1 (all breakfast calculations) 
/*
ADD Unique ID([State-Reporting]+"@"+STR([School ID])+"@"+STR([District ID]) (e.g. DE-###-###) ##Calc: combine state-reporting, school ID, and district ID, separated in middle with a hyphen 
*/
-- drop table #tmp_UID

SELECT
	row_num,
    "District ID",
    CONCAT('WI', '-', '0', "School ID", '-', "District ID") as "Unique ID"
INTO #tmp_UID
FROM #join1

-- select * from #join1 --Lunch
-- select * from #join2 --Brekfast
-- select * from #tmp_UID

/*
ADD NCES ID(National Center for Education Statistics (NCES) School/site ID number 
*/
-- Fetch this from WI thrid raw file 
-- drop table #tmp_NCES
SELECT
	row_num,
	"Unique ID",
    CASE WHEN len("district id") = 6 THEN "district id" 
				ELSE  RIGHT ('000000'+ "district id",6) END as "NCES ID"
INTO #tmp_NCES
FROM #tmp_UID

-- select * from #tmp_UID
-- select * from #tmp_NCES (all Unique and NCES ID)

/*
ADD School Year (SY##-## (e.g. SY16-17) calculate using claim date 
*/

ALTER TABLE #join2 DROP COLUMN "School Year"
SET ANSI_WARNINGS  OFF

ALTER TABLE #join2
ADD School_Year varchar NULL

UPDATE #join2 SET School_Year = 17-18 
WHERE School_Year = NULL 

SELECT * FROM #join2
/*
ADD Target Area (it is NULL in WIsconsin Clean Data set) 
*/

ALTER TABLE #join2
ADD "Target Area"  varchar(MAX) NULL

/*
ADD FR Enrollment (Use [Enrollment-Free and Reduced] when it exist,,, IF [Enrollment-Free and Reduced] is missing then use [Enrollment-Free]+ [Enrollment-Reduced],,,
if both of them null then [FR Enrollment] will stay NULL
*/

SELECT row_num, "CEP (Y/N)", "Lunch Meals-Free", "Lunch Meals-Paid", "Enrollment-Total",
	CAST("Enrollment-Free" as float) + CAST("Enrollment-Reduced" as float) as "FR Enrollment"
INTO ##tmp_enroll
FROM #join1

--select * from ##tmp_enroll

select * from #join1 --Lunch
select * from #join2 --Brekfast
/*
ADD FR Enrollment Percentage( IF CEP (Y/N) = “N” then [FR Enrollment] / [Enrollment-Total],,, IF CEP (Y/N) = “Y” then [Lunch Meals-Free] / ([Lunch Meals-Free] + [Lunch Meals-Paid]),,,
if [Lunch Meals-Free] is missing then use [Lunch Meals-Free and Reduced] / ([Lunch Meals-Free and Reduced] + [Lunch Meals-Paid]),,, if both null then NULL
*/
-- drop table ##tmp_enroll1
SELECT row_num, "FR Enrollment", CASE WHEN "CEP (Y/N)" = 'N'
	THEN CAST("FR Enrollment" as float) / CAST("Enrollment-Total" as float)
	WHEN "CEP (Y/N)" = 'Y'
	THEN CAST("Lunch Meals-Free" as float) / (CAST("Lunch Meals-Free" as float) + CAST("Lunch Meals-Paid" as float))
	ELSE NULL
	END as "FR Enrollment Percentage"
	INTO ##tmp_enroll1
FROM ##tmp_enroll

-- SELECT * FROM ##tmp_enroll1 (all enrollment calculations) 

/*
ADD School Level-Standardized( USE School Level Original and change whatever row is not mtching) 
*/
drop table #tmp_std
SELECT row_num,"School Level-Original","School Type-Original", CASE WHEN "School Level-Original" = 'High School'
	THEN 'High'
	WHEN "School Level-Original" = 'Elementary/Sec Combined' 
	THEN 'Other'
	WHEN "School Level-Original" = 'RCCI'
	THEN 'Other'
	WHEN "School Level-Original" = 'Unknown'
	THEN 'Unknown'
	WHEN "School Level-Original" = NULL
	THEN 'Unknown'
	WHEN "School Level-Original" = 'Elementary School' 
	THEN 'Primary'
	WHEN "School Level-Original" = 'Junior H.S' 
	THEN 'Middle/High'
	WHEN "School Level-Original" = 'Middle School' 
	THEN 'Middle'
	ELSE NULL
	END as "School Level-Standardized"
	INTO #tmp_std
FROM #join1

-- SELECT * FROM #tmp_std
/*
ADD School Type-Standardized( Use School Type-Original and match with case When) 
*/

SELECT row_num, "School Level-Standardized", CASE WHEN "School Type-Original" = 'Public'
	THEN 'Public'
	WHEN "School Type-Original" = 'Nonpublic'
	THEN 'Nonpublic'
	WHEN "School Type-Original" = 'Public RCCI'
	THEN 'Other'
	ELSE NULL
	END as "School Type-Standardized"
	INTO #tmp_std1
FROM #tmp_std

-- SELECT * FROM #tmp_std1 (all the Standardized table for school level and school type) 
/*
ADD Breakfast Delivery Model from State Agency Tracking-Standardized

P="Y" and O, Q, R, S, and T = "N", then "Breakfast Delivery Model from State Agency-Original"="MID_MORNING_MODEL". 
However, if if P="Y", Q="Y" and O, R, S, and T = "N", then "Breakfast Delivery Model from State Agency-Original"="MID_MORNING_MODEL, CLASSROOM_MODEL".
*/

select * from #join1 --Lunch
select * from #join2 --Brekfast

/********
Final 64 column table
*********/
--Lunch
SELECT a.*, b."Fr Lunch Meals", b."FR Lunch ADP"
INTO #final
FROM #join1 a
JOIN ##tmp_lunch1 b ON a.row_num = b.row_num
--BRF
SELECT a.*, b."Fr Breakfast Meals", b."FR Breakfast ADP"
INTO #final1 
FROM #final a
JOIN ##tmp_brf1 b ON a.row_num = b.row_num

-- NCES
SELECT a.*, b."Unique ID", b."NCES ID"
INTO #final2 
FROM #final1 a
JOIN #tmp_NCES b ON a.row_num = b.row_num


--##tmp_enroll1
SELECT a.*, b."FR Enrollment", b."FR Enrollment Percentage"
INTO #final3 
FROM #final2 a
JOIN ##tmp_enroll1 b ON a.row_num = b.row_num


--#tmp_std1
SELECT a.*, b."School Level-Standardized", b."School Type-Standardized"
INTO #final4 
FROM #final3 a
JOIN #tmp_std1 b ON a.row_num = b.row_num


SELECT DISTINCT a.*,b.* 

FROM #final4 a
JOIN #join2 b ON a."school name" = b."school name" AND a."claim date" = b."claim date" 
				AND CASE WHEN len(a."district id") = 6 THEN a."district id" 
				ELSE  RIGHT ('000000'+a."district id",6) END 
	
				= 
	  
				CASE WHEN len(b."district id") = 6 THEN b."district id" 
				ELSE  RIGHT ('000000'+b."district id",6) END 

select * from #final4
select * from #join2
select count(*) from #final4
/********
QA Example Logic
*********/
CREATE TABLE #test_compare1 (field1 varchar(max), field2 varchar(max))

INSERT INTO #test_compare1 VALUES ('A', 'B')
INSERT INTO #test_compare1 VALUES ('A', 'C')
INSERT INTO #test_compare1 VALUES ('A', 'D')
INSERT INTO #test_compare1 VALUES ('B', 'D')
INSERT INTO #test_compare1 VALUES ('B', 'F')



CREATE TABLE #test_compare2 (field1 varchar(max), field2 varchar(max))

INSERT INTO #test_compare2 VALUES ('A', 'B')
INSERT INTO #test_compare2 VALUES ('A', 'C')
INSERT INTO #test_compare2 VALUES ('A', 'D')
INSERT INTO #test_compare2 VALUES ('B', 'D')
INSERT INTO #test_compare2 VALUES ('E', 'F')


SELECT COUNT(*) FROM #test_compare1

SELECT COUNT(*) FROM #test_compare2

SELECT COUNT(*)
FROM
(
SELECT * FROM #test_compare1
INTERSECT
SELECT * FROM #test_compare2
)a

