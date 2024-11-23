![Banner](https://www.upload.ee/image/17422045/pulsea-banner.png)
# pulsea.db

A secure, multi-format database with built-in encryption, data validation, and automatic backups.

## Features

- 🔐 **Secure Storage**: Built-in encryption for sensitive data
- 📁 **Flexible Storage Format**: Choose between YAML, JSON, or SQL format
- 🗜️ **Data Compression**: Automatic zlib compression for efficient storage
- 🔄 **Automatic Backups**: Configurable backup intervals with retention policies
- 📊 **Table Support**: SQL-like table operations with validation and relations
- 🔍 **Rich Query API**: Complex queries with where clauses, ordering, and pagination
- 🛡️ **Data Validation**: Schema validation with type checking and custom rules
- 🔗 **Relationship Support**: Define and maintain relationships between tables
- 💾 **Auto-save**: Automatic saving of changes (configurable)
- 🚀 **Async/Promise Based**: Modern asynchronous API

## Installation

```bash
npm install pulsea.db
```

## Quick Start

```javascript
const PulseaDB = require('pulsea.db');

// Initialize the database with encryption and file format
const db = new PulseaDB({
    file: 'database.json',      // Choose format: database.json, database.yml, or database.sql
    encryption: {
        secretKey: 'your-secret-key'
    },
    dir: './data',              // Optional: default is './data'
    autoSave: true,             // Optional: default is true
    debug: true                 // Optional: enables debug logging
});

// Basic Operations
await db.set('user.1', { name: 'John', age: 30 });
const user = await db.get('user.1');
console.log(user); // { name: 'John', age: 30 }
```

## Table Operations

### Creating a Table

```javascript
await db.createTable({
    name: 'users',
    columns: ['name', 'email', 'age'],
    validations: {
        name: { type: 'string', pattern: '^[A-Za-z ]+$' },
        email: { type: 'string', pattern: '^[^@]+@[^@]+\.[^@]+$' },
        age: { type: 'number', min: 0, max: 120 }
    },
    indexes: ['email'],
    relations: {
        groupId: { table: 'groups', column: 'id' }
    }
});
```

### Querying Data

```javascript
// Insert data
await db.set('users.1', {
    name: 'John Doe',
    email: 'john@example.com',
    age: 30
});

// Query with conditions
const users = await db.query('users', {
    where: {
        age: { $gt: 25 },
        name: { $like: 'John%' }
    },
    orderBy: 'age desc',
    limit: 10,
    offset: 0
});
```

## Detailed API Reference

### Core Database Methods

#### Constructor
```javascript
const db = new PulseaDB({
    file: 'database.json',        // Required: database file with format extension
    encryption: {
        secretKey: 'required-key' // Required: encryption key
    },
    dir: './data',               // Optional: database directory
    debug: false,                // Optional: debug mode
    autoSave: true,              // Optional: auto-save changes
    backupInterval: 3600000,     // Optional: backup interval (ms)
    maxBackups: 5,               // Optional: maximum backups to keep
    enableAutoBackup: true       // Optional: enable automatic backups
});
```

The constructor initializes a new PulseaDB instance with the following features:
- Single file format support (YAML, JSON, or SQL)
- Automatic directory creation for database and backups
- File locking mechanism for concurrent operations
- Built-in encryption with zlib compression
- Auto-backup scheduling if enabled

### Storage Optimization

PulseaDB automatically optimizes storage using zlib compression. This feature:
- Reduces database file size by 50-90% depending on data type
- Compresses data before encryption for maximum efficiency
- Automatically handles compression/decompression transparently
- Works with all storage formats (JSON, YAML, SQL)
- Maintains data integrity with efficient binary compression

### Security Mechanism

PulseaDB implements a robust security mechanism to protect critical database operations:
- 🔒 **Method Protection**: Core methods are locked and cannot be modified
- 🛡️ **Class Protection**: Database class structure is frozen and immutable
- 🔐 **Instance Protection**: Each database instance has its own protected method registry
- ⚡ **Runtime Verification**: Critical method calls are verified during execution
- 🚫 **Tamper Detection**: Attempts to modify protected methods trigger security errors

Protected methods include:
- `encryptValue`: Data encryption
- `decryptValue`: Data decryption
- `validateTableData`: Data validation
- `sanitizePath`: Path security
- `init`: Initialization process

### Data Management

##### `set(key, value)`
Stores an encrypted value in the database.
```javascript
// Simple key-value storage
await db.set('user.preferences', { theme: 'dark' });

// Table record storage
await db.set('users.123', {
    name: 'John',
    email: 'john@example.com'
});
```
- Validates key format and type
- Encrypts values automatically
- Supports nested objects and arrays
- Handles table data with validation
- Auto-saves if enabled

##### `get(key, defaultValue = null)`
Retrieves and decrypts a value from the database.
```javascript
// Get with default value
const theme = await db.get('user.preferences.theme', 'light');

// Get table record
const user = await db.get('users.123');
```
- Returns decrypted data
- Supports deep object paths
- Returns defaultValue if key doesn't exist
- Handles table metadata and relations

##### `delete(key)`
Removes a key and its value from the database.
```javascript
// Delete user
await db.delete('users.123');

// Delete nested value
await db.delete('user.preferences.theme');
```
- Cleans up empty parent objects
- Returns boolean indicating success
- Auto-saves if enabled

### Backup Operations

#### `backup()`
Creates a backup of the database in the selected format.
```javascript
const backupPath = await db.backup();
console.log(`Backup created at: ${backupPath}`);
```
- Creates timestamped backup file
- Maintains specified number of backups
- Supports all storage formats

#### `restoreFromBackup(backupPath)`
Restores the database from a backup file.
```javascript
await db.restoreFromBackup('./data/backups/backup-2024-01-01.json');
```
- Validates backup file format
- Restores data with encryption
- Maintains data integrity

### Connect With Us

- GitHub: [github.com/pulseainc](https://github.com/pulseainc/pulsea.db)
- Bluesky: [@pulseainc](https://bsky.app/profile/pulseainc.bsky.social)