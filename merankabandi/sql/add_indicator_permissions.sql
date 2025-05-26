-- SQL script to add Merankabandi indicator permissions to OpenIMIS
-- Run this script against your OpenIMIS database to register the permissions

-- Section Management Permissions (160005-160008)
INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160005, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160005
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160006, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160006
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160007, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160007
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160008, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160008
);

-- Indicator Management Permissions (160009-160012)
INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160009, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator', 'User')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160009
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160010, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160010
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160011, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160011
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160012, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160012
);

-- Indicator Achievement Management Permissions (160013-160016)
INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160013, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator', 'User')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160013
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160014, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator', 'User')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160014
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160015, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator', 'IMIS Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160015
);

INSERT INTO tblRoleRight (RoleID, RightID, ValidityFrom, AuditUserID)
SELECT RoleID, 160016, GETDATE(), -1
FROM tblRole
WHERE RoleName IN ('Administrator')
AND NOT EXISTS (
    SELECT 1 FROM tblRoleRight 
    WHERE tblRoleRight.RoleID = tblRole.RoleID 
    AND RightID = 160016
);

-- Verification query to check permissions were added
SELECT r.RoleName, rr.RightID, 
    CASE rr.RightID
        WHEN 160005 THEN 'Search and view sections'
        WHEN 160006 THEN 'Create new sections'
        WHEN 160007 THEN 'Update sections'
        WHEN 160008 THEN 'Delete sections'
        WHEN 160009 THEN 'Search and view indicators'
        WHEN 160010 THEN 'Create indicators'
        WHEN 160011 THEN 'Update indicators'
        WHEN 160012 THEN 'Delete indicators'
        WHEN 160013 THEN 'Search and view achievements'
        WHEN 160014 THEN 'Create achievements'
        WHEN 160015 THEN 'Update achievements'
        WHEN 160016 THEN 'Delete achievements'
    END AS RightDescription
FROM tblRole r
INNER JOIN tblRoleRight rr ON r.RoleID = rr.RoleID
WHERE rr.RightID BETWEEN 160005 AND 160016
AND rr.ValidityTo IS NULL
ORDER BY r.RoleName, rr.RightID;