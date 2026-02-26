# ArieSQL: The Modern SQL Agent

## Introduction

ArieSQL is a SQL agent designed to provide secure, efficient, and user-friendly access to databases. It enforces strict access controls, ensuring that users can only interact with data they are authorized to see. With support for multiple database dialects and a focus on security, ArieSQL is the ideal solution for organizations looking to manage their data access effectively.

## Application Flow

<image src="images/ariesql_flow.png" alt="ArieSQL Application Flow" width="100%"/>

## Quick Start

### 1. **Install Dependencies**

#### 1.1. Backend Dependencies (Python, managed by uv)

```
uv sync
```

Fill in the env variables in `.env` file:

```
cp .env.example .env
```

#### 1.2. Frontend Dependencies (Node.js, managed by npm)

```
npm install
```

#### 1.3. Database Manifest

The database manifest is a user-defined access scope declaration in order for the SQL Agent to work securely.
Refer to the `./configs/database_manifest.mssql.example.json` for the JSON structure.
After filling the database manifest, insert its path into the `DATABASE_MANIFEST_PATH` env variable.

Copy the example:

```
cp database_manifest.mssql.example.json database_manifest.mssql.json
```

### 2. **Run the Application**

#### 2.1. Start the Database and Cache Storage (using Docker Compose)

```
docker compose up -d
```

#### 2.2. Start the Backend Server

```
uv run main.py
```

#### 2.3. Start the Frontend Development Server

```
cd frontend && npm run dev
```
