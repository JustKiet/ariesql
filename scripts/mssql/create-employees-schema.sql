-- ============================================================
-- TSQL Schema: employees database
-- Migrated from PostgreSQL employees dataset
-- ============================================================

USE employees;
GO

-- Create the employees schema (equivalent to PG's employees schema)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'employees')
BEGIN
    EXEC('CREATE SCHEMA employees');
END
GO

-- ============================================================
-- Table: employees.employee
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'employee' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.employee (
        id          BIGINT        NOT NULL IDENTITY(1,1),
        birth_date  DATE          NOT NULL,
        first_name  NVARCHAR(14)  NOT NULL,
        last_name   NVARCHAR(16)  NOT NULL,
        gender      NVARCHAR(1)   NOT NULL CHECK (gender IN ('M', 'F')),
        hire_date   DATE          NOT NULL,
        CONSTRAINT PK_employee PRIMARY KEY (id)
    );
END
GO

-- ============================================================
-- Table: employees.department
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'department' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.department (
        id          CHAR(4)       NOT NULL,
        dept_name   NVARCHAR(40)  NOT NULL,
        CONSTRAINT PK_department PRIMARY KEY (id),
        CONSTRAINT UQ_dept_name UNIQUE (dept_name)
    );
END
GO

-- ============================================================
-- Table: employees.department_employee
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'department_employee' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.department_employee (
        employee_id     BIGINT  NOT NULL,
        department_id   CHAR(4) NOT NULL,
        from_date       DATE    NOT NULL,
        to_date         DATE    NOT NULL,
        CONSTRAINT PK_department_employee PRIMARY KEY (employee_id, department_id),
        CONSTRAINT FK_dept_emp_employee FOREIGN KEY (employee_id)
            REFERENCES employees.employee (id),
        CONSTRAINT FK_dept_emp_department FOREIGN KEY (department_id)
            REFERENCES employees.department (id)
    );

    CREATE NONCLUSTERED INDEX IX_dept_emp_department_id
        ON employees.department_employee (department_id);
END
GO

-- ============================================================
-- Table: employees.department_manager
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'department_manager' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.department_manager (
        employee_id     BIGINT  NOT NULL,
        department_id   CHAR(4) NOT NULL,
        from_date       DATE    NOT NULL,
        to_date         DATE    NOT NULL,
        CONSTRAINT PK_department_manager PRIMARY KEY (employee_id, department_id),
        CONSTRAINT FK_dept_mgr_employee FOREIGN KEY (employee_id)
            REFERENCES employees.employee (id),
        CONSTRAINT FK_dept_mgr_department FOREIGN KEY (department_id)
            REFERENCES employees.department (id)
    );

    CREATE NONCLUSTERED INDEX IX_dept_mgr_department_id
        ON employees.department_manager (department_id);
END
GO

-- ============================================================
-- Table: employees.salary
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'salary' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.salary (
        employee_id BIGINT  NOT NULL,
        amount      BIGINT  NOT NULL,
        from_date   DATE    NOT NULL,
        to_date     DATE    NOT NULL,
        CONSTRAINT PK_salary PRIMARY KEY (employee_id, from_date),
        CONSTRAINT FK_salary_employee FOREIGN KEY (employee_id)
            REFERENCES employees.employee (id)
    );
END
GO

-- ============================================================
-- Table: employees.title
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'title' AND schema_id = SCHEMA_ID('employees'))
BEGIN
    CREATE TABLE employees.title (
        employee_id BIGINT      NOT NULL,
        title       NVARCHAR(50) NOT NULL,
        from_date   DATE        NOT NULL,
        to_date     DATE        NULL,
        CONSTRAINT PK_title PRIMARY KEY (employee_id, title, from_date),
        CONSTRAINT FK_title_employee FOREIGN KEY (employee_id)
            REFERENCES employees.employee (id)
    );
END
GO

PRINT 'Schema creation complete.';
GO
