-- Analysis of aACE6_Data.db relationships

-- Check for potential company references in Invoices
SELECT 'Checking Invoices table for company/contact references...' as Analysis;
.mode column
.headers on

-- Sample Invoices data to understand structure
SELECT
    _RECORD_RecID,
    _RECORD_ParentRecID,
    TaskGroupRecID,
    TaskGroupItemRecID,
    AssignedToTeamMemberRecID,
    LICodeRecID,
    OfficeDepartmentRecID
FROM Invoices
WHERE _RECORD_RecID IS NOT NULL
LIMIT 5;

-- Sample InvoiceItems data
SELECT 'Checking InvoiceItems table structure...' as Analysis;
SELECT
    _RECORD_RecID,
    _RECORD_ParentRecID,
    TaskGroupRecID,
    _VL_TaskGroupRecID
FROM InvoiceItems
WHERE _RECORD_RecID IS NOT NULL
LIMIT 5;

-- Sample Companies data
SELECT 'Checking Companies table structure...' as Analysis;
SELECT
    _RECORD_RecID,
    _RECORD_ParentRecID,
    JobRecID,
    OrderRecID,
    LICodeRecID,
    OfficeDepartmentRecID
FROM Companies
WHERE _RECORD_RecID IS NOT NULL
LIMIT 5;

-- Check if there are any Contacts references
SELECT 'Checking Contacts table...' as Analysis;
SELECT COUNT(*) as ContactsCount FROM Contacts;

-- Check Jobs table for relationships
SELECT 'Checking Jobs table...' as Analysis;
SELECT COUNT(*) as JobsCount FROM Jobs;

-- Check Orders table
SELECT 'Checking Orders table...' as Analysis;
SELECT COUNT(*) as OrdersCount FROM Orders;