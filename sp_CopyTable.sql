USE [master]
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_CopyTable]
   @schemaName sysname
   , @tableName sysname
   , @destinationSchemaName sysname
   , @destinationTableName sysname = NULL
   , @updateColumnNames BIT = 1 --Only applies when new Destination Table Name, will update all columns that contained the old name with the new name
   
   , @destinationDatabase sysname = NULL
   , @replaceExistingTable BIT = 1

   , @createPrimaryKeys BIT = 1
   , @createDefaultConstraints BIT = 1
   , @createIndexes BIT = 1
   , @createIdentity BIT = 1
   , @createTable BIT = 1

   , @returnExecuteScript BIT = 0
   , @executeScript BIT = 1
   , @includeData BIT = 1
AS
BEGIN
   SET NOCOUNT ON;

   IF @destinationTableName IS NULL
   BEGIN
      SELECT @destinationTableName = @tableName;
   END

   IF @destinationDatabase IS NULL
   BEGIN
      SELECT @destinationDatabase = DB_NAME();
   END

   --Start of object Validation

   --Source Table And Schema validation
   IF NOT EXISTS
   (
      SELECT 1
      FROM sys.tables AS t
      INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
      WHERE s.name = @schemaName
         AND t.name = @tableName
   )
   BEGIN
      RAISERROR ('Source Table [%s].[%s] Does Not Exist' , 11, 1, @schemaName, @tableName);
      RETURN -1;
   END

   --Destination Database
   IF NOT EXISTS
   (
      SELECT 1
      FROM sys.databases AS d
      WHERE d.name = @destinationDatabase
   )
   BEGIN
      RAISERROR ('Destination Database [%s] Does Not Exist' , 11, 1, @destinationDatabase);
      RETURN -1;
   END

   --Destination Schema
   DECLARE @destinationSchemaExistsScript NVARCHAR(MAX);
   DECLARE @destinationSchemaExists BIT = 0;

   SELECT @destinationSchemaExistsScript = N'SELECT @DoesSchemaExist = 1
                                             FROM ' + @destinationDatabase + '.sys.schemas AS s 
                                             WHERE s.name = ''' + @destinationSchemaName + ''';';
   
   EXECUTE sp_executesql @stmt = @destinationSchemaExistsScript, @params = N'@DoesSchemaExist BIT OUTPUT', @DoesSchemaExist = @destinationSchemaExists OUTPUT;

   IF @destinationSchemaExists = 0
   BEGIN
      RAISERROR ('Destination Database Schema [%s] Does Not Exist' , 11, 1, @destinationSchemaName);
      RETURN -1;
   END

   --Destination Table exists but @replaceExistingTable = 0
   DECLARE @destinationTableExistsScript NVARCHAR(MAX);
   DECLARE @destinationTableExists BIT = 0;

   SELECT @destinationTableExistsScript = N'SELECT @DoesTableExist = 1
                                             FROM ' + @destinationDatabase + '.sys.tables AS t
                                             INNER JOIN ' + @destinationDatabase + '.sys.schemas AS s ON t.schema_id = s.schema_id
                                             WHERE s.name = ''' + @destinationSchemaName + '''
                                                AND t.name = ''' + @destinationTableName + ''';';
   
   EXECUTE sp_executesql @stmt = @destinationTableExistsScript, @params = N'@DoesTableExist BIT OUTPUT', @DoesTableExist = @destinationTableExists OUTPUT;

   IF @destinationTableExists = 1 AND @replaceExistingTable = 0
   BEGIN
      RAISERROR ('Destination Table already exists [%s].[%s], but @replaceExistingTable is set to 0' , 11, 1, @destinationSchemaName, @destinationTableName);
      RETURN -1;
   END

   --End object Validation

   DECLARE @defaultCollation SYSNAME;

   SELECT @defaultCollation = d.collation_name
   FROM sys.databases AS d
   WHERE d.database_id = DB_ID();

   DECLARE @commandText NVARCHAR(MAX) = '';
   DECLARE @fullCommandText NVARCHAR(MAX) = '';
   DECLARE @tableCreateScript NVARCHAR(MAX) = '';
   DECLARE @indexCreateScript NVARCHAR(MAX) = '';
   DECLARE @replaceExistingTableScript NVARCHAR(MAX) = '';

   IF @createTable = 1
   BEGIN
      SELECT @tableCreateScript =
         'CREATE TABLE ' + @destinationSchemaName + '.' + @destinationTableName + '('
            + STUFF(
            (
               SELECT ', ' + CASE
                                 WHEN @tableName != @destinationTableName AND @updateColumnNames = 1 THEN QUOTENAME(REPLACE(c.name, @tableName, @destinationTableName))
                                 ELSE QUOTENAME(c.name)
                              END + ' '
                  + CASE
                        --CHAR or VARCHAR 'c' ommited intentionally from LIKE statement - also BINARY or VARBINARY
                        WHEN types.Name LIKE '[^n]%har'
                           OR types.name LIKE '%binary' THEN  UPPER(types.NAME) + '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS NVARCHAR(4)) END + ')'
                        --NCHAR or NVARCHAR
                        WHEN types.Name LIKE 'n%char' THEN  UPPER(types.NAME) + '('+ CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS NVARCHAR(4)) END + ')'
        
                        WHEN types.Name = 'datetime2' THEN 'DATETIME2(' + CAST(c.scale AS NVARCHAR(2)) + ')'        
                        WHEN types.Name = 'datetimeoffset' THEN 'DATETIMEOFFSET(' + CAST(c.scale AS NVARCHAR(2)) + ')'        

                        WHEN types.Name = 'float' THEN 'FLOAT(' + CAST(c.precision AS NVARCHAR(2)) + ')'
                        WHEN types.Name = 'decimal' THEN 'DECIMAL(' + CAST(c.precision AS NVARCHAR(2)) + ', ' + + CAST(c.scale AS NVARCHAR(2)) + ')'
                        WHEN types.Name = 'numeric' THEN 'NUMERIC(' + CAST(c.precision AS NVARCHAR(2)) + ', ' + + CAST(c.scale AS NVARCHAR(2)) + ')'
                        ELSE UPPER(types.NAME) --Default, just show the dataType
                     END
                  + CASE
                        WHEN @createDefaultConstraints = 1
                           AND dc.name IS NOT NULL
                        THEN ' CONSTRAINT ' + CASE
                                                WHEN @tableName != @destinationTableName AND @updateColumnNames = 1 THEN REPLACE(dc.name, @tableName, @destinationTableName)
                                                ELSE dc.name
                                             END + ' DEFAULT ' + dc.definition
                        ELSE ''
                     END
                  + CASE
                        WHEN @createIdentity = 1
                           AND c.is_identity = 1
                           THEN ' IDENTITY(' + CAST(ic.seed_value AS NVARCHAR(20)) + ', ' + CAST(ic.increment_value AS NVARCHAR(20)) + ')'
                        ELSE ''
                     END
                  + CASE
                       WHEN @defaultCollation != c.collation_name
                       THEN ' COLLATE ' + c.collation_name
                       ELSE ''
                    END
                  + CASE
                        WHEN c.is_nullable = 1 THEN ' NULL'
                        ELSE ' NOT NULL'
                     END
               FROM sys.columns AS c
               INNER JOIN sys.types AS types ON c.user_type_id = types.user_type_id
               LEFT OUTER JOIN sys.default_constraints AS dc ON dc.parent_object_id = t.object_id
                  AND dc.parent_column_id = c.column_id
               LEFT OUTER JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id
               WHERE t.object_id = c.object_id
               ORDER BY c.column_id ASC
               FOR XML PATH('')
            ), 1, 2, '')
            + ')'
            + CASE
                  WHEN p.data_compression = 1 THEN ' WITH (DATA_COMPRESSION = ROW);'
                  WHEN p.data_compression = 2 THEN ' WITH (DATA_COMPRESSION = PAGE);'
                  WHEN p.data_compression = 3 THEN ' CREATE CLUSTERED COLUMNSTORE INDEX ' + i.name + ' ON ' + @destinationSchemaName + '.' + @destinationTableName + ' WITH (DATA_COMPRESSION = COLUMNSTORE);'
                  WHEN p.data_compression = 4 THEN ' CREATE CLUSTERED COLUMNSTORE INDEX ' + i.name + ' ON ' + @destinationSchemaName + '.' + @destinationTableName + ' WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE);'
                  ELSE ';'
               END
      FROM sys.tables AS t
      INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
      INNER JOIN sys.indexes AS i ON t.object_id = i.object_id
      INNER JOIN sys.partitions AS p ON t.object_id = p.object_id
      WHERE p.index_id IN (0, 1)
         AND p.partition_number = 1
         AND i.index_id IN (0, 1)
         AND s.name = @schemaName
         AND t.name = @tableName
   END
   
   IF @createIndexes = 1 OR @createPrimaryKeys = 1
   BEGIN
      SELECT @indexCreateScript = COALESCE(STUFF((
      SELECT  
         COALESCE(STUFF(
         (
         SELECT DISTINCT ' ' +
         CASE
            WHEN i.is_primary_key = 1
               THEN 'ALTER TABLE ' + @destinationSchemaName + '.' + @destinationTableName + ' ADD CONSTRAINT '
               + REPLACE(i.name, @tableName, @destinationTableName) + ' PRIMARY KEY '
               + CASE WHEN i.type_desc = 'CLUSTERED'
                        THEN 'CLUSTERED '
                        ELSE 'NONCLUSTERED '
               END
               + '( '
            ELSE 'CREATE '
               + CASE WHEN i.is_primary_key = 0 AND i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END
               + CASE WHEN i.type_desc = 'CLUSTERED'
                        THEN 'CLUSTERED '
                     WHEN i.type_desc = 'CLUSTERED COLUMNSTORE'
                        THEN NULL -- Not doing CCI since we load it during the table create, NULL gets caught by COALESCE to remove CCI repeat
                     WHEN i.type_desc = 'NONCLUSTERED'
                        THEN 'NONCLUSTERED '
                     WHEN i.type_desc = 'NONCLUSTERED COLUMNSTORE'
                        THEN 'NONCLUSTERED COLUMNSTORE '
                        ELSE ''
               END
               + 'INDEX ' + REPLACE(i.name, @tableName, @destinationTableName) + ' ON ' + @destinationSchemaName + '.' + @destinationTableName
               + CASE WHEN i.type_desc != 'CLUSTERED COLUMNSTORE' THEN ' ( ' ELSE '' END
         END +
         CASE
            WHEN i.type_desc != 'CLUSTERED COLUMNSTORE'
               THEN
                  COALESCE(STUFF(
                  (
                     SELECT ', ' + CASE
                                    WHEN @tableName != @destinationTableName AND @updateColumnNames = 1 THEN QUOTENAME(REPLACE(clist.name, @tableName, @destinationTableName))
                                    ELSE QUOTENAME(clist.name)
                                 END + CASE WHEN indexColumnList.is_descending_key = 0 THEN ' ASC' ELSE ' DESC' END
                     FROM sys.index_columns AS indexColumnList
                     INNER JOIN sys.columns AS clist ON indexColumnList.column_id = clist.column_id
                        AND indexColumnList.object_id = clist.object_id
                     WHERE indexColumnList.index_id = ic.index_id
                        AND indexColumnList.object_id = i.object_id
                        AND (indexColumnList.is_included_column = 0
                           OR i.type_desc = 'Nonclustered Columnstore') -- Columnstore columns are noted with 1 for is_included_column)
                     ORDER BY c.column_id ASC
                     FOR XML PATH('')
                     ), 1, 2, ''), '')
                  + ') ' -- standard index columns
                  +  COALESCE('INCLUDE (' + STUFF(
                  (
                     SELECT ', ' + CASE
                                       WHEN @tableName != @destinationTableName AND @updateColumnNames = 1 THEN QUOTENAME(REPLACE(clist.name, @tableName, @destinationTableName))
                                       ELSE QUOTENAME(clist.name)
                                    END
                     FROM sys.index_columns AS indexColumnList
                     INNER JOIN sys.columns AS clist ON indexColumnList.column_id = clist.column_id
                        AND indexColumnList.object_id = clist.object_id
                     WHERE indexColumnList.index_id = ic.index_id
                        AND indexColumnList.object_id = i.object_id
                        AND (indexColumnList.is_included_column = 1
                           AND i.type_desc != 'Nonclustered Columnstore') -- Columnstore columns are noted with 1 for is_included_column)
                     ORDER BY c.column_id ASC
                     FOR XML PATH('')
                     ), 1, 2, '') + ' )', '') -- closing Include syntax here with the );
                  + CASE
                        WHEN p.data_compression = 1 THEN ' WITH (DATA_COMPRESSION = ROW);'
                        WHEN p.data_compression = 2 THEN ' WITH (DATA_COMPRESSION = PAGE);'
                        ELSE ';'
                     END
            ELSE
               CASE
                  WHEN p.data_compression = 3 THEN ' WITH (DATA_COMPRESSION = COLUMNSTORE);'
                  WHEN p.data_compression = 4 THEN ' WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE);'
                  ELSE ';'
               END -- Clustered Columnstore indexes don't need the above
         END
         FROM sys.columns AS c
         LEFT JOIN sys.index_columns AS ic ON c.column_id = ic.column_id
         WHERE i.index_id = ic.index_id
            AND t.object_id = c.object_id
            AND i.is_primary_key = CASE
                                    WHEN @createIndexes = 1 THEN
                                       CASE WHEN @createPrimaryKeys = 1
                                               THEN i.is_primary_key
                                               ELSE 0
                                       END
                                    WHEN @createIndexes = 0 AND @createPrimaryKeys = 1
                                       THEN 1
                                       ELSE i.is_primary_key
                                    END -- Set value to deal with variables about what to create
         AND OBJECT_SCHEMA_NAME(i.object_id) = @schemaName -- Only deal with tables/objects in the correct schema noted above
         FOR XML PATH('')
         ), 1, 1, ''), '')
      FROM sys.tables AS t
      INNER JOIN sys.schemas AS s ON t.schema_id = s.SCHEMA_ID
      INNER JOIN sys.indexes AS i ON t.OBJECT_ID = i.object_id
      INNER JOIN sys.partitions AS p ON t.object_id = p.object_id
      WHERE p.index_id IN (0, 1)
         AND p.partition_number = 1
         AND s.name = @schemaName
         AND t.name = @tableName
      FOR XML PATH('')
      ), 1, 0, ''), '')
   END

   IF @createTable = 1
   BEGIN
      IF @destinationDatabase IS NOT NULL -- Add DB if required
      BEGIN
         SELECT @commandText = 'USE ' + @destinationDatabase + '; ';
      END

      IF @replaceExistingTable = 1
      BEGIN
         SELECT @replaceExistingTableScript = 'DROP TABLE IF EXISTS ' + @destinationSchemaName + '.' + @destinationTableName + '; ';
      END

      SELECT @commandText = @commandText + @replaceExistingTableScript + @tableCreateScript;

      IF @executeScript = 1
      BEGIN
         EXECUTE sp_executesql @stmt = @commandText;
      END

      SELECT @fullCommandText = @commandText; -- Setting full commandText to have table DDL
   END

   IF @includeData = 1
   BEGIN

      DECLARE @requiresIdentityInsert BIT = 0;
      DECLARE @insertDataScript NVARCHAR(MAX) = '';

      IF @createIdentity = 1
      BEGIN
         SELECT @requiresIdentityInsert = 1
         FROM sys.schemas AS s
         INNER JOIN sys.tables AS t ON s.schema_id = t.schema_id
         INNER JOIN sys.columns AS c ON t.object_id = c.object_id
         WHERE t.object_id = c.object_id
            AND s.name = @schemaName
            AND t.name = @tableName
            AND c.is_identity = 1
      END

      DECLARE @sourceColumns NVARCHAR(MAX);
      DECLARE @destinationColumns NVARCHAR(MAX);

      SELECT @sourceColumns = STUFF(
         (
            SELECT ', ' + QUOTENAME(c.name)
            FROM sys.schemas AS s
            INNER JOIN sys.tables AS t ON s.schema_id = t.schema_id
            INNER JOIN sys.columns AS c ON t.object_id = c.object_id
            LEFT OUTER JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id
            WHERE t.object_id = c.object_id
               AND s.name = @schemaName
               AND t.name = @tableName
            ORDER BY c.column_id ASC
            FOR XML PATH('')
         ), 1, 2, '')

      SELECT @destinationColumns = STUFF(
         (
            SELECT ', ' + CASE
                              WHEN @tableName != @destinationTableName AND @updateColumnNames = 1 THEN QUOTENAME(REPLACE(c.name, @tableName, @destinationTableName))
                              ELSE QUOTENAME(c.name)
                           END
            FROM sys.schemas AS s
            INNER JOIN sys.tables AS t ON s.schema_id = t.schema_id
            INNER JOIN sys.columns AS c ON t.object_id = c.object_id
            LEFT OUTER JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id
            WHERE t.object_id = c.object_id
               AND s.name = @schemaName
               AND t.name = @tableName
            ORDER BY c.column_id ASC
            FOR XML PATH('')
         ), 1, 2, '')


      IF @destinationDatabase IS NOT NULL -- Add DB if required
      BEGIN
         SELECT @insertDataScript = 'USE ' + @destinationDatabase + '; ';
      END

      IF @requiresIdentityInsert = 1
      BEGIN
         SELECT @insertDataScript = @insertDataScript + 'SET IDENTITY_INSERT ' + @destinationSchemaName + '.' + @destinationTableName + ' ON; '
      END

      SELECT @insertDataScript = @insertDataScript +
                         N'INSERT INTO ' + @destinationSchemaName + '.' + @destinationTableName + N' WITH (TABLOCK)
                           ( ' +
                              @destinationColumns +
                           ')
                           SELECT ' + @sourceColumns + ' FROM ' + DB_NAME() + '.' + @schemaName + N'.' + @tableName + '; ';
      

      IF @requiresIdentityInsert = 1
      BEGIN
         SELECT @insertDataScript = @insertDataScript + 'SET IDENTITY_INSERT ' + @destinationSchemaName + '.' + @destinationTableName + ' OFF; '
      END
      
      IF @executeScript = 1
      BEGIN
         EXECUTE sp_executesql @stmt = @insertDataScript;
      END

      SELECT @fullCommandText = @fullCommandText + @insertDataScript; --Adding potential data migration into full script
   END

   IF @indexCreateScript IS NOT NULL AND LEN(@indexCreateScript) > 0
   BEGIN
      IF @destinationDatabase IS NOT NULL -- Add DB if required
      BEGIN
         SELECT @commandText = 'USE ' + @destinationDatabase + '; ' + @indexCreateScript;
      END

      IF @executeScript = 1
      BEGIN
         EXECUTE sp_executesql @stmt = @commandText;
      END
      
      SELECT @fullCommandText = @fullCommandText + @commandText
   END

   IF @ReturnExecuteScript = 1 
   BEGIN
      SELECT @fullCommandText;
   END
END
