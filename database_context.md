# Database Schema Context

Database: employees
Schema: employees

## Tables

### department
- Row count: 9
- Table size: 40 kB
- Primary keys: id

Columns:
  - id: character(4) NOT NULL [9 unique (unique cardinality), length: 4-4]
  - dept_name: character varying(40) NOT NULL [9 unique (unique cardinality), length: 5-18]

### department_employee
- Row count: 331603
- Table size: 29 MB
- Primary keys: employee_id, department_id

Columns:
  - employee_id: bigint NOT NULL [300024 unique (high cardinality), avg=253332.61]
  - department_id: character(4) NOT NULL [9 unique (low cardinality), length: 4-4]
  - from_date: date NOT NULL
  - to_date: date NOT NULL

### department_manager
- Row count: 24
- Table size: 40 kB
- Primary keys: employee_id, department_id

Columns:
  - employee_id: bigint NOT NULL [24 unique (unique cardinality), avg=110780.83]
  - department_id: character(4) NOT NULL [9 unique (low cardinality), length: 4-4]
  - from_date: date NOT NULL
  - to_date: date NOT NULL

### employee
- Row count: 300024
- Table size: 26 MB
- Primary keys: id

Columns:
  - id: bigint NOT NULL [300024 unique (unique cardinality), avg=253321.76]
  - birth_date: date NOT NULL
  - first_name: character varying(14) NOT NULL [1275 unique (low cardinality), length: 3-14]
  - last_name: character varying(16) NOT NULL [1637 unique (low cardinality), length: 4-16]
  - gender: USER-DEFINED NOT NULL [2 unique (low cardinality)]
  - hire_date: date NOT NULL

### salary
- Row count: 2844047
- Table size: 227 MB
- Primary keys: employee_id, from_date

Columns:
  - employee_id: bigint NOT NULL [300024 unique (low cardinality), avg=253057.44]
  - amount: bigint NOT NULL [85814 unique (low cardinality), avg=63810.74]
  - from_date: date NOT NULL
  - to_date: date NOT NULL

### title
- Row count: 443308
- Table size: 44 MB
- Primary keys: employee_id, title, from_date

Columns:
  - employee_id: bigint NOT NULL [300024 unique (medium cardinality), avg=253075.03]
  - title: character varying(50) NOT NULL [7 unique (low cardinality), length: 5-18]
  - from_date: date NOT NULL
  - to_date: date NULL

## Relationships

- department_employee.department_id -> department.id
- department_employee.employee_id -> employee.id
- department_manager.department_id -> department.id
- department_manager.employee_id -> employee.id
- salary.employee_id -> employee.id
- title.employee_id -> employee.id
