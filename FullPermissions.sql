DROP TABLE IF EXISTS #IndividualLogin;

CREATE TABLE #IndividualLogin
(
   AccountName sysname NOT NULL
   , LoginType VARCHAR(8) NULL
   , Privlege VARCHAR(8) NULL
   , MappedLoginName sysname NULL
   , PermissionPath sysname NULL
);

DROP TABLE IF EXISTS #WindowsGroup;

CREATE TABLE #WindowsGroup
(
   WindowsGroupId INT IDENTITY(1, 1) NOT NULL
   , GroupName sysname
);

DROP TABLE IF EXISTS #RoleUser;

CREATE TABLE #RoleUser
(
   UserWithAccess NVARCHAR(128) NOT NULL
   , AccessType NVARCHAR(256) NOT NULL
   , GrantedObject NVARCHAR(128) NULL
   , UserTypeDescription NVARCHAR(128) NOT NULL
);

DROP TABLE IF EXISTS #IndividualPermission;

CREATE TABLE #IndividualPermission
(
   DatabaseRole NVARCHAR(128) NOT NULL
   , UserInRole NVARCHAR(128) NOT NULL
   , UserTypeDescription NVARCHAR(128) NOT NULL
);

INSERT INTO #RoleUser
(
   UserWithAccess, AccessType, GrantedObject, UserTypeDescription
)
SELECT dbprin.name AS UserWithAccess
   , CASE
        WHEN dbperm.state_desc != 'GRANT' THEN dbperm.state_desc + ' ' + dbperm.permission_name
        ELSE dbperm.permission_name
     END AS AccessType
   , CASE dbperm.class_desc
        WHEN 'SCHEMA' THEN 'SCHEMA::[' + SCHEMA_NAME(dbperm.major_id) + ']'
        WHEN 'OBJECT_OR_COLUMN' THEN
           CASE
              WHEN dbperm.minor_id = 0 -- Object
   THEN
                 '[' + OBJECT_SCHEMA_NAME(dbperm.major_id) + '].' + '[' + OBJECT_NAME(dbperm.major_id) + ']' COLLATE Latin1_General_CI_AS_KS_WS
              ELSE -- Column
           (
              SELECT OBJECT_NAME(object_id) + ' ON (' + name + ')'
              FROM sys.columns
              WHERE object_id = dbperm.major_id
                 AND column_id = dbperm.minor_id
           )
           END
        WHEN 'Database' THEN ''
     END AS GrantedObject
   , dbprin.type_desc
FROM sys.database_permissions AS dbperm
JOIN sys.database_principals AS dbprin ON dbperm.grantee_principal_id = dbprin.principal_id
LEFT JOIN sys.objects AS o ON o.object_id = dbperm.major_id
WHERE dbperm.major_id >= 0
   AND dbperm.permission_name NOT IN
(
   'Connect'
);
--   AND dbprin.name NOT LIKE '%_writer';

INSERT INTO #IndividualPermission
(
   DatabaseRole, UserInRole, UserTypeDescription
)
SELECT su.name AS DatabaseRole, dp.name AS UserInRole, dp.type_desc
FROM sys.database_role_members AS drm
INNER JOIN sys.database_principals AS dp ON drm.member_principal_id = dp.principal_id
INNER JOIN sys.sysusers AS su ON drm.role_principal_id = su.uid;
--WHERE su.name NOT LIKE '%_writer';

INSERT INTO #WindowsGroup
(
   GroupName
)
SELECT DISTINCT ip.UserInRole
FROM #IndividualPermission AS ip
WHERE ip.UserTypeDescription = 'WINDOWS_GROUP'
UNION
SELECT DISTINCT ru.UserWithAccess
FROM #RoleUser AS ru
WHERE ru.UserTypeDescription = 'WINDOWS_GROUP';

DECLARE @index INT;

SELECT @index = MAX(wg.WindowsGroupId)
FROM #WindowsGroup AS wg;

DECLARE @groupName sysname;

WHILE @index > 0
BEGIN
   SELECT @groupName = wg.GroupName
   FROM #WindowsGroup AS wg
   WHERE wg.WindowsGroupId = @index;

   BEGIN TRY

      INSERT INTO #IndividualLogin
      (
         AccountName, LoginType, Privlege, MappedLoginName, PermissionPath
      )
      EXECUTE sys.xp_logininfo @groupName, 'members';
   END TRY
   BEGIN CATCH
      DECLARE @errorMessage NVARCHAR(1000) = ERROR_MESSAGE();
      DECLARE @errorCode NVARCHAR(10);
      IF ERROR_NUMBER() = 15404
         IF SUBSTRING(@errorMessage, PATINDEX('%error code 0x%', @errorMessage) + 11, LEN(@errorMessage) -  (PATINDEX('%error code 0x%', @errorMessage) + 11)) = '0x2147'
            INSERT INTO #IndividualLogin
            (
               AccountName, LoginType, Privlege, MappedLoginName, PermissionPath
            )
            SELECT 'ERROR - SG Failed to resolve, has at least one non-US domain item', 'user', 'user', 'ERROR - SG Failed to resolve, has at least one non-US domain item', @groupName
         ELSE
            THROW;
      ELSE
         THROW;
   END CATCH

   SELECT @index = @index - 1;
END;

SELECT DISTINCT DB_NAME() AS DatabaseName
   , COALESCE(MemberOfGroup.AccountName, ip.UserInRole, ru.UserWithAccess) AS UserOrGroupWithAccess
   , ip.UserInRole AS UserInRole
   , COALESCE(ru.UserWithAccess, ip.DatabaseRole) AS AccessVia
   , ru.AccessType
   , ru.GrantedObject
FROM #RoleUser AS ru
FULL OUTER JOIN #IndividualPermission AS ip ON ru.UserWithAccess = ip.DatabaseRole
OUTER APPLY
(
   SELECT il.AccountName, il.LoginType, il.Privlege, il.MappedLoginName, il.PermissionPath
   FROM #IndividualLogin AS il
   WHERE ip.UserInRole = il.PermissionPath
) AS MemberOfGroup
--WHERE ru.GrantedObject LIKE '%scorecard%'
ORDER BY UserOrGroupWithAccess ASC, ip.UserInRole ASC, AccessVia ASC, ru.AccessType ASC, ru.GrantedObject ASC;