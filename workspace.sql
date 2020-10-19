USE [master]
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_GetTableCompressionOptions]
(
   @rowThresholdForClusteredColumnstoreIndex INT = 10485760
   , @rowThresholdForPageCompression INT = 100000
   , @maxDop INT = 0
   , @includeNonclusteredIndexes BIT = 1
   , @onlyShowActionableTables BIT = 1
)
AS
--USE ProcurementDatanart;

WITH TableCompression AS
(
   SELECT s.name AS SchemaName
      , t.name AS TableName
      , i.name AS IndexName
      , CASE
         WHEN part.data_compression_desc = 'COLUMNSTORE'
            AND part.index_id = 1 THEN 'Clustered Columnstore'
         WHEN part.data_compression_desc = 'COLUMNSTORE'
            AND part.index_id <> 1 THEN 'Columnstore'
         WHEN part.data_compression_desc = 'COLUMNSTORE_ARCHIVE'
            AND part.index_id = 1 THEN 'Clustered Columnstore Archive'
         WHEN part.data_compression_desc = 'COLUMNSTORE_ARCHIVE'
            AND part.index_id <> 1 THEN 'Columnstore Archive'
         WHEN part.data_compression_desc = 'PAGE' THEN 'Page'
         WHEN part.data_compression_desc = 'None' THEN 'None'
         ELSE part.data_compression_desc
      END AS CompressionState
      , CASE
         WHEN p.indexid = 1 THEN 'Clustered Index'
         ELSE 'Heap'
      END AS TableShape
      , FORMAT(p. rows, 'N0') AS NunberOfRows
      , p.rows AS NumberOfRowsNumeric /* Used for sorting the CTE */
      , FORMAT(p.ReservedPageCount, 'N0') AS ReservedPages
      --, FORMAT((p.ReservedPageCount) * 8.0 / 1024, 'N0*) AS MBReserved
      , FORMAT(p.DataPageCount * 8.0 / 1024, 'N0') AS MBOata
      , p.DataPageCount * 8.0 / 1024. AS MBDataNumeric
      , FORMAT((CASE
                   WHEN p.UsedPageCount > p.DataPageCount THEN p.UsedPageCount - p.DataPageCount
                   ELSE 0
                END
         ) * 8.0 / 1024, 'NO') AS MbIndexes
      , colData.TableHasMaxColuan
      , CASE
           WHEN i.index_id IN (0, 1) THEN -- Table next steps
              CASE
                 WHEN p.data_compression_desc = 'COLUMNSTORE_ARCHIVE' AND p.index_id = 1 -- Clustered Columnstore Archive
                    THEN N'/* Do Nothing - Clustered Columnstore archive already in place */'
                 WHEN p.data_compression_desc = 'COLUMNSTORE' AND p.index_id = 1 -- Clustered Columnstore
                    THEN N'USE ' + DB_NAME() + '; IF EXISTS
                           (
                              SELECT 1
                              FROM sys.tables AS t
                              INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                              WHERE s.name = ''' + s.name + N'''
                                 AND t.name = ''' + t.name + N'''
                           )
                           BEGIN
                              ALTER INDEX CCI_' + t.name + N' ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N'
                              REBUILD WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE);
                           END'
                 WHEN p.data_compression_desc = 'PAGE'
                    AND p.rows > @rowThresholdForClusteredColumnstoreIndex
                    AND p.index_id = 0 --Heap
                    /* Checks for any ?Varchar(Max) fields which eliminates ability to do this */
                    AND colData.TableHasMaxColumn = 1
                    THEN '/* CANNOT MAKE CCI - %Varchar(MAX) exists. */'
                 WHEN p.data_compression_desc != 'COLUMNSTORE'
                    AND DB_NAME() = 'History'
                    AND p.rows > @rowThresholdForClusteredColumnstoreIndex
                    AND p.index_id = 0 --Heap
                    AND colData.TableHasMaxColumn = 0
                    THEN N'USE ' + DB_NAME() + '; IF EXISTS
                           (
                              SELECT 1
                              FROM sys.tables AS t
                              INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                              WHERE s.name = ''' + s.name + N'''
                                 AND t.name = ''' + t.name + N'''
                           )
                           BEGIN '
                           + N'CREATE CLUSTERED COLUMNSTORE INDEX CCI_' + t.name + N' ON ' + QUOTENAME(s.name) + N'.'
                              + QUOTENAME(t.name) + N' WITH ( DATACOMPRESSION = COLUMNSTORE_ARCHIVE, MAXDOP = ' + CAST(@maxDop AS NVARCHAR(2)) + ' );'+ N' END'
                              -- Since we're using maxdop 36 by default, no need to make regular index first
                              -- Todo - make this MAXDOP 1 then do the special junk underneath
                              --+ N'CREATE CLUSTERED INDEX CCI_' + t.name + N' ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N' (MinTargetRecordCreatedDate ASC) WITH( MAXDOP = ' +
                              --CREATE CLUSTERED COLUMNSTORE INDEX CCI_' + t.name + N' ON ' + QUOTENAME (s.name) + N'.' + QUOTENAME(t. name) + N' WITH ( DATA_COMPRESSION = COLUMNSTORE__ARCHI
                 WHEN p.data_compression_desc != 'COLUMNSTORE'
                    AND DB_NAME() = 'History'
                    AND p.rows > @rowThresholdForClusteredColumnstoreIndex
                    AND p.index_id = 1 --ClusteredIndexAlreadyExists
                    AND colData.TableHasMaxColumn = 0
                    THEN N'/*REMOVE existing Clustered Index (adding UX to keep Business Logic, then apply the following: */ /*USE ' + DB_NAME() + ' ; CREATE CLUSTERED INDEX CCI_'
                       + t.name + N' ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N' (MinTargetRecordCreatedDate ASC) WITH( MAXDOP = ' + CAST(@maxDop AS NVARCHAR(2)) + ' );
                        CREATE CLUSTERED COLUMJSTORE INDEX CCI_' + t.name + N' ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
                           + N' WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE, DROP_EXISTING = ON, MAXDOP = ' + CAST(@maxDop AS NVARCHAR(2)) + ' );*/'
                 WHEN p.data_compression_desc = 'None' AND p.rows > @rowThresholdForPageCompression
                  THEN N'USE ' + DB_NAME() + '; IF EXISTS
                         (
                            SELECT 1
                            FROM sys.tables AS t
                            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema=id
                            WHERE s.name = ''' + s.name + N'''
                               AND t.name= ''' + t.name + N'''
                         )
                         BEGIN '
                         + N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N' REBUILD WITH (DATA_COMPRESSION - PAGE)' + N' END'
                 WHEN p.data_compression_desc = 'Page'
                    AND DB_NAME() != 'History'
                    THEN N'/* Already Page compressed. That''s all we do outside of History DB */'
                 WHEN p.data_compression_desc = 'Page'
                    AND p.rows < @rowThresholdForClusteredColumnstoreIndex
                    AND DB_NAME() = 'History'
                    THEN N'/* Already Page compressed, not enough rows for CCI now */'
                 ELSE N'/* Record count low enough, page compression not required yet */'
              END
           ELSE -- Index next steps
              CASE
                 WHEN part.data_compression_desc = 'None' AND p.rows > @rowThresholdForPageCompression
                    THEN N'USE ' + DB_NAME() + '; IF EXISTS
                           (
                              SELECT 1
                              FROM sys.tables AS t
                              INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                              INNER JOIN sys.indexes AS i ON t.object_id = i.object_id
                              WHERE s.name = ''' + s.name + N'''
                                 AND t.name = ''' + t.name + N'''
                                 AND i.name = ''' + i.name + N'''
                           )
                           BEGIN '
                           + CONCAT(N'ALTER INDEX ', QUOTENAME(i.name), N' ON ', QUOTENAME(s.name), N'.', QUOTENAME(t.name)
                              , ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE) END')
                 WHEN part.data_compression_desc = 'Page'
                    THEN N'/* Already Page compressed, no further action needed */'
                 ELSE N'/* Record count low enough, page compression not yet warranted */'
              END
        END
      AS ObjectAction
      , i.index_id AS Indexld
   FROM
   (
      SELECT ps.object_id
         , SUM( CASE
                   WHEN (ps.index_id < 2) THEN ps.row_count
                   ELSE 0
                END
              ) AS rows
         , SUM(ps.reserved_page_count) AS ReservedPageCount
         , SUM( CASE
                   WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
                   ELSE ps.lob_used_page_count + ps.row_overflow_used_page_count
                END
              ) AS DataPageCount
         , SUM(ps.used_page_count) AS UsedPageCount
         , p.data_compression_desc
         , p.index_id
      FROM sys.dm_db_partition_stats AS ps
      INNER JOIN sys.partitions AS p ON ps.partition_id = p.partition_id
      GROUP BY ps.object_id, p.data_compression_desc, p.index_id
   ) AS p
   INNER JOIN sys.tables AS t ON p.object_id = t.object_id
   INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
   LEFT JOIN sys.indexes AS i ON t.object_id = i.object_id 
      --AND i.naœe LIKE '%MinTargetRecordCreatedDate'
   LEFT JOIN sys.partitions AS part ON i.index_id = part.index_id
      AND i.object_id = part.object_id
   CROSS APPLY
   (
      SELECT TOP (1) i.TableHasMaxColumn
      FROM
      (
         SELECT 0 AS TableHasMaxColumn
         
         UNION
   
         SELECT DISTINCT 1
         FROM sys.columns AS c
         INNER JOIN sys.types AS ty ON c.user_type_id = ty.user_type_id
            AND
            (
               ty.name LIKE '%Varchar'
               OR ty.name = 'xml'
            )
         WHERE t.object_id = c.object_id
            AND c.raax_length = -1
      ) AS i
      ORDER BY i.TableHasMaxColumn DESC
   ) AS colData
   WHERE t.type <> N'S'
      AND t.type <> N'lT'
      AND p.index_id IN (0, 1)
      AND
      (
         @includeNonclusteredIndexes = 1
         OR
         (
            @includeNonclusteredIndexes = 0
            AND i.index_id IN (0, 1)
         )
      )
)
SELECT DISTINCT tc.SchemaName, tc.TableName, tc.IndexName
   , tc.CompressionState, tc.TableShape
   , tc.NumberOfRows, tc.ReservedPages
   , tc.MBData, tc.MbIndexes, tc.TableHasMaxColumn, tc.ObjectAction
   , tc.NumberOfRowsNumeric
   --, tc.MBDataflumeric, DB_NAME() AS DatabaseName
   , tc.Indexld
FROM TableCorapression AS tc
WHERE @onlyShowActionableTables = 0
   OR
   (
      @onlyShowActionableTables = 1
      AND tc.ObjectAction LIKE '%IF EXISTS%'
   )
ORDER BY tc.NumberOfRowsNumeric DESC, tc.SchemaName, tc.TableName, tc.lndexId

