const fs = require('fs');
const yaml = require('js-yaml');
const path = require('path');
const zlib = require('zlib');
const { promisify } = require('util');
const { DatabaseError } = require('../exceptions/Error');
const Encryption = require('../helpers/Encryption');
const { protectClass, initializeProtection, checkMethodProtection } = require('./protection/Protection');
const compress = promisify(zlib.deflate);
const decompress = promisify(zlib.inflate);

class PulseaDB {
    constructor(options = {}) {
        initializeProtection(this);

        if (!options.encryption?.secretKey) {
            throw new DatabaseError('Encryption key is required. Please provide it through environment variables.');
        }

        if (options.file) {
            const ext = path.extname(options.file).toLowerCase();
            if (!['.yml', '.yaml', '.json', '.sql'].includes(ext)) {
                throw new DatabaseError('Invalid file format. Supported formats are: yml, yaml, json, sql');
            }
            this.fileFormat = ext.replace('.', '');
            if (this.fileFormat === 'yaml') this.fileFormat = 'yml';
        } else {
            throw new DatabaseError('File option is required. Example: { file: "database.json" }');
        }

        this.dir = path.resolve(options.dir || './data');
        this.debug = options.debug || false;
        this.autoSave = options.autoSave !== false;
        this.backupIntervalTime = options.backupInterval || 3600000;
        this.maxBackups = options.maxBackups || 5;
        this.backupDir = path.join(this.dir, 'backups');
        this.data = {};
        this.backupIntervalId = null;
        this.fileLock = new Map();
        this.filePath = this.sanitizePath(path.join(this.dir, options.file));
        this.encryption = new Encryption(options.encryption.secretKey);
        this.initialized = this.init().catch(error => {
            this.debug && DatabaseError.info('Failed to initialize database: ' + this.sanitizeErrorMessage(error.message));
            throw error;
        });

        if (options.enableAutoBackup) {
            this.startAutoBackup();
        }
    }

    sanitizePath(filepath) {
        const normalized = path.normalize(filepath);
        if (normalized.includes('..') || !normalized.startsWith(this.dir)) {
            throw new DatabaseError('Invalid file path: Directory traversal not allowed');
        }
        return normalized;
    }

    sanitizeErrorMessage(message) {
        return message.replace(/\/.*\//, '[PATH]/');
    }

    async acquireLock(filepath) {
        while (this.fileLock.get(filepath)) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        this.fileLock.set(filepath, true);
    }

    releaseLock(filepath) {
        this.fileLock.delete(filepath);
    }

    async init() {
        try {
            await fs.promises.mkdir(this.dir, { recursive: true });
            await fs.promises.mkdir(this.backupDir, { recursive: true });

            if (!fs.existsSync(this.filePath)) {
                const emptyContent = this.fileFormat === 'json' ? '{}' : 
                                   this.fileFormat === 'yml' ? '' :
                                   '';
                await fs.promises.writeFile(this.filePath, emptyContent);
            }

            await this.load();
            return true;
        } catch (error) {
            throw new DatabaseError('Failed to initialize database: ' + error.message);
        }
    }

    async info() {
        await this.ensureInitialized();

        const stats = await fs.promises.stat(this.filePath);
        const fileSizeInBytes = stats.size;
        const fileSizeInKB = (fileSizeInBytes / 1024).toFixed(2);
        const fileSizeInMB = (fileSizeInBytes / (1024 * 1024)).toFixed(2);
        const lastModified = stats.mtime;
        const tables = await this.listTables();
        const totalRecordCount = tables.reduce((sum, table) => sum + table.rowCount, 0);
        const latestBackup = this.findLatestBackup();
        const backupCount = fs.readdirSync(this.backupDir)
            .filter(file => file.startsWith('backup-') && file.endsWith(`.${this.fileFormat}`)).length;

        return {
            databasePath: this.filePath,
            fileFormat: this.fileFormat,
            fileSize: {
                bytes: fileSizeInBytes,
                kilobytes: fileSizeInKB,
                megabytes: fileSizeInMB
            },
            lastModified: lastModified.toISOString(),
            tables: {
                count: tables.length,
                details: tables
            },
            totalRecordCount,
            backups: {
                count: backupCount,
                latestBackup: latestBackup,
                backupDirectory: this.backupDir
            },
            encryption: {
                enabled: true,
                method: 'K9Crypt'
            },
            autoSave: this.autoSave,
            debugMode: this.debug
        };
    }

    async load() {
        try {
            const fileContent = await fs.promises.readFile(this.filePath, 'utf8');
            
            if (fileContent.trim()) {
                if (this.fileFormat === 'json') {
                    this.data = JSON.parse(fileContent) || {};
                } else if (this.fileFormat === 'yml') {
                    this.data = yaml.load(fileContent) || {};
                } else if (this.fileFormat === 'sql') {
                    this.data = {};
                }
            } else {
                this.data = {};
            }

            this.debug && DatabaseError.info('Database loaded successfully');
            return true;
        } catch (error) {
            if (error.code === 'ENOENT') {
                this.data = {};
                return true;
            }
            throw new DatabaseError('Failed to load database: ' + error.message);
        }
    }

    async save() {
        await this.ensureInitialized();
        try {
            if (!fs.existsSync(this.dir)) {
                await fs.promises.mkdir(this.dir, { recursive: true });
            }

            await this.acquireLock(this.filePath);
            try {
                const tempFile = this.filePath + '.tmp';
                let content = '';

                if (this.fileFormat === 'json') {
                    content = JSON.stringify(this.data, null, 2);
                } else if (this.fileFormat === 'yml') {
                    content = yaml.dump(this.data);
                } else if (this.fileFormat === 'sql') {
                    content = await this.generateSQLContent();
                }

                await fs.promises.writeFile(tempFile, content);
                await fs.promises.rename(tempFile, this.filePath);
            } finally {
                this.releaseLock(this.filePath);
            }

            this.debug && DatabaseError.info('Database saved successfully');
            return true;
        } catch (error) {
            throw new DatabaseError('Failed to save database: ' + this.sanitizeErrorMessage(error.message));
        }
    }

    async encryptValue(value) {
        checkMethodProtection(this, 'encryptValue');

        try {
            if (value === null || value === undefined) {
                return value;
            }
            const stringValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
            const compressedValue = await compress(Buffer.from(stringValue));
            return await this.encryption.encrypt(compressedValue.toString('base64'));
        } catch (error) {
            this.debug && DatabaseError.warn(`Compression/Encryption failed: ${error.message}`);
            return value;
        }
    }

    async decryptValue(value) {
        checkMethodProtection(this, 'decryptValue');

        try {
            if (!value || typeof value !== 'string') {
                return value;
            }
            const decrypted = await this.encryption.decrypt(value);
            const decompressedValue = await decompress(Buffer.from(decrypted, 'base64'));
            const decodedValue = decompressedValue.toString();
            
            try {
                return JSON.parse(decodedValue);
            } catch (e) {
                return decodedValue;
            }
        } catch (error) {
            this.debug && DatabaseError.warn(`Decryption/Decompression failed: ${error.message}`);
            return value;
        }
    }

    async set(key, value) {
        await this.ensureInitialized();
        if (!key) throw new DatabaseError('Key is required');
        if (typeof key !== 'string') throw new DatabaseError('Key must be a string');

        const [tableName, rowId, ...rest] = key.split('.');

        if (rowId && !rest.length && rowId !== '_meta') {
            const meta = this.data[tableName]?._meta;
            if (meta) {
                await this.validateTableData(tableName, value);

                const encryptedData = {};
                for (const [field, val] of Object.entries(value)) {
                    encryptedData[field] = await this.encryptValue(val);
                }

                if (!this.data[tableName]) {
                    this.data[tableName] = { _meta: meta };
                }
                this.data[tableName][rowId] = encryptedData;

                meta.rowCount = Object.keys(this.data[tableName]).filter(k => k !== '_meta').length;

                if (this.autoSave) await this.save();
                return value;
            }
        }

        const keys = key.split('.');
        let current = this.data;

        for (let i = 0; i < keys.length - 1; i++) {
            if (!(keys[i] in current)) {
                current[keys[i]] = {};
            }
            current = current[keys[i]];
        }

        const lastKey = keys[keys.length - 1];
        current[lastKey] = await this.encryptValue(value);

        if (this.autoSave) await this.save();
        return value;
    }

    async get(key, defaultValue = null) {
        await this.ensureInitialized();
        if (!key) throw new DatabaseError('Key is required');
        if (typeof key !== 'string') throw new DatabaseError('Key must be a string');

        const [tableName, rowId, ...rest] = key.split('.');

        if (rowId && !rest.length) {
            const meta = this.data[tableName]?._meta;
            if (meta) {
                if (rowId === '_meta') {
                    return meta;
                }

                const encryptedData = this.data[tableName]?.[rowId];
                if (!encryptedData) return defaultValue;
                const decryptedData = {};
                for (const [field, val] of Object.entries(encryptedData)) {
                    decryptedData[field] = await this.decryptValue(val);
                }
                return decryptedData;
            }
        }

        const keys = key.split('.');
        let current = this.data;

        for (const k of keys) {
            if (current === undefined || current === null || !(k in current)) {
                return defaultValue;
            }
            current = current[k];
        }

        return await this.decryptValue(current);
    }

    fetch(key, defaultValue = null) {
        return this.get(key, defaultValue);
    }

    has(key) {
        return this.get(key) !== null;
    }

    async delete(key) {
        if (!key) throw new DatabaseError('Key is required');
        if (typeof key !== 'string') throw new DatabaseError('Key must be a string');

        const keys = key.split('.');
        let current = this.data;
        const stack = [];

        for (let i = 0; i < keys.length - 1; i++) {
            if (!(keys[i] in current)) {
                return false;
            }
            stack.push({ obj: current, key: keys[i] });
            current = current[keys[i]];
        }

        const deleted = delete current[keys[keys.length - 1]];

        for (let i = stack.length - 1; i >= 0; i--) {
            const { obj, key } = stack[i];
            if (Object.keys(obj[key]).length === 0) {
                delete obj[key];
            }
        }

        if (this.autoSave) await this.save();
        return deleted;
    }

    async clear() {
        this.data = {};
        if (this.autoSave) await this.save();
        return true;
    }

    async push(key, value) {
        if (!key) throw new DatabaseError('Key is required');
        const arr = await this.get(key, []);
        if (!Array.isArray(arr)) {
            throw new DatabaseError('Target is not an array');
        }
        arr.push(value);
        return this.set(key, arr);
    }

    async pull(key, value) {
        if (!key) throw new DatabaseError('Key is required');
        const arr = await this.get(key);
        if (!Array.isArray(arr)) {
            throw new DatabaseError('Target is not an array');
        }
        const newArr = arr.filter(item => item !== value);
        return this.set(key, newArr);
    }

    async add(key, value) {
        if (!key) throw new DatabaseError('Key is required');
        if (typeof value !== 'number') throw new DatabaseError('Value must be a number');
        const currentValue = await this.get(key, 0);
        if (typeof currentValue !== 'number') {
            throw new DatabaseError('Target is not a number');
        }
        return this.set(key, currentValue + value);
    }

    async subtract(key, value) {
        return this.add(key, -value);
    }

    async multiply(key, value) {
        if (!key) throw new DatabaseError('Key is required');
        if (typeof value !== 'number') throw new DatabaseError('Value must be a number');
        const currentValue = await this.get(key, 0);
        if (typeof currentValue !== 'number') {
            throw new DatabaseError('Target is not a number');
        }
        return this.set(key, currentValue * value);
    }

    async divide(key, value) {
        if (!key) throw new DatabaseError('Key is required');
        if (typeof value !== 'number') throw new DatabaseError('Value must be a number');
        if (value === 0) throw new DatabaseError('Cannot divide by zero');
        const currentValue = await this.get(key, 0);
        if (typeof currentValue !== 'number') {
            throw new DatabaseError('Target is not a number');
        }
        return this.set(key, currentValue / value);
    }

    async increment(key) {
        return this.add(key, 1);
    }

    async decrement(key) {
        return this.add(key, -1);
    }

    type(key) {
        const value = this.get(key);
        return value === null ? null : typeof value;
    }

    size() {
        return Object.keys(this.data).length;
    }

    all() {
        return this.data;
    }

    async deleteAll() {
        return this.clear();
    }

    keys() {
        return Object.keys(this.data);
    }

    values() {
        return Object.values(this.data);
    }

    entries() {
        return Object.entries(this.data);
    }

    find(predicate) {
        for (const [key, value] of this.entries()) {
            if (predicate(value, key)) {
                return { key, value };
            }
        }
        return null;
    }

    filter(predicate) {
        const result = {};
        for (const [key, value] of this.entries()) {
            if (predicate(value, key)) {
                result[key] = value;
            }
        }
        return result;
    }

    map(callback) {
        const result = {};
        for (const [key, value] of this.entries()) {
            result[key] = callback(value, key);
        }
        return result;
    }

    forEach(callback) {
        for (const [key, value] of this.entries()) {
            callback(value, key);
        }
    }

    some(predicate) {
        for (const [key, value] of this.entries()) {
            if (predicate(value, key)) {
                return true;
            }
        }
        return false;
    }

    every(predicate) {
        for (const [key, value] of this.entries()) {
            if (!predicate(value, key)) {
                return false;
            }
        }
        return true;
    }

    async backup() {
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const backupPath = path.join(this.backupDir, `backup-${timestamp}.${this.fileFormat}`);

            const decryptedData = {};
            for (const [key, value] of Object.entries(this.data)) {
                if (key === '_meta') {
                    decryptedData[key] = value;
                } else {
                    decryptedData[key] = await this.decryptValue(value);
                }
            }

            let backupContent;
            if (this.fileFormat === 'json') {
                backupContent = JSON.stringify(decryptedData, null, 2);
            } else if (this.fileFormat === 'yml') {
                backupContent = yaml.dump(decryptedData);
            } else if (this.fileFormat === 'sql') {
                backupContent = await this.generateSQLContent(decryptedData);
            }

            await fs.promises.writeFile(backupPath, backupContent);

            this.cleanOldBackups();
            this.debug && DatabaseError.info('Backup created successfully');
            return backupPath;
        } catch (error) {
            throw new DatabaseError('Failed to create backup: ' + error.message);
        }
    }

    async restoreFromBackup(backupPath) {
        try {
            const ext = path.extname(backupPath).toLowerCase();
            if (ext !== `.${this.fileFormat}`) {
                throw new DatabaseError(`Backup file format does not match the database format: ${ext} !== .${this.fileFormat}`);
            }

            const fileContent = await fs.promises.readFile(backupPath, 'utf8');

            let data;
            if (this.fileFormat === 'json') {
                data = JSON.parse(fileContent);
            } else if (this.fileFormat === 'yml') {
                data = yaml.load(fileContent);
            } else if (this.fileFormat === 'sql') {
                data = {};
            }

            for (const [key, value] of Object.entries(data)) {
                await this.set(key, value);
            }

            this.debug && DatabaseError.info('Restored from backup: ' + backupPath);
            return true;
        } catch (error) {
            throw new DatabaseError('Failed to restore from backup: ' + error.message);
        }
    }

    findLatestBackup() {
        try {
            const backups = fs.readdirSync(this.backupDir)
                .filter(file => file.startsWith('backup-') && file.endsWith(`.${this.fileFormat}`))
                .sort()
                .reverse();

            const latestBackup = backups[0];
            return latestBackup ? path.join(this.backupDir, latestBackup) : null;
        } catch (error) {
            return null;
        }
    }

    cleanOldBackups() {
        try {
            const backups = fs.readdirSync(this.backupDir)
                .filter(file => file.startsWith('backup-') && file.endsWith(`.${this.fileFormat}`))
                .sort()
                .reverse();

            if (backups.length > this.maxBackups) {
                backups.slice(this.maxBackups).forEach(file => {
                    fs.unlinkSync(path.join(this.backupDir, file));
                });
            }
        } catch (error) {
            this.debug && DatabaseError.info('Failed to clean old backups: ' + error.message);
        }
    }

    async createTable({ name, columns, validations = {}, indexes = [], relations = {} }) {
        await this.ensureInitialized();
        if (!name) throw new DatabaseError('Table name is required');
        if (!columns || !Array.isArray(columns) || columns.length === 0) {
            throw new DatabaseError('Columns must be a non-empty array');
        }

        if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
            throw new DatabaseError('Invalid table name. Use only letters, numbers, and underscores, starting with a letter');
        }

        if (await this.tableExists(name)) {
            throw new DatabaseError(`Table '${name}' already exists`);
        }

        const uniqueColumns = [...new Set(columns)];
        for (const column of uniqueColumns) {
            if (typeof column !== 'string' || !/^[a-zA-Z][a-zA-Z0-9_]*$/.test(column)) {
                throw new DatabaseError('Invalid column name: ' + column);
            }
        }

        for (const [column, rules] of Object.entries(validations)) {
            if (!uniqueColumns.includes(column)) {
                throw new DatabaseError(`Validation rule specified for non-existent column: ${column}`);
            }
            if (rules.type && !['string', 'number', 'boolean', 'object'].includes(rules.type)) {
                throw new DatabaseError(`Invalid type for column ${column}: ${rules.type}`);
            }
        }

        const uniqueIndexes = [...new Set(indexes)];
        for (const index of uniqueIndexes) {
            if (!uniqueColumns.includes(index)) {
                throw new DatabaseError(`Index specified for non-existent column: ${index}`);
            }
        }

        for (const [column, relation] of Object.entries(relations)) {
            if (!uniqueColumns.includes(column)) {
                throw new DatabaseError(`Relation specified for non-existent column: ${column}`);
            }
            if (!relation.table || !relation.column) {
                throw new DatabaseError(`Invalid relation specification for column: ${column}`);
            }
            if (!this.data[relation.table]?._meta) {
                throw new DatabaseError(`Related table does not exist: ${relation.table}`);
            }
            if (!this.data[relation.table]._meta.columns.includes(relation.column)) {
                throw new DatabaseError(`Related column does not exist: ${relation.column} in table ${relation.table}`);
            }
        }

        this.data[name] = {
            _meta: {
                columns: uniqueColumns,
                validations,
                indexes: uniqueIndexes,
                relations,
                created: new Date().toISOString(),
                rowCount: 0
            }
        };

        if (this.autoSave) await this.save();
        this.debug && DatabaseError.info(`Table '${name}' created with columns: ${uniqueColumns.join(', ')}`);
        return true;
    }

    async validateTableData(tableName, data) {
        checkMethodProtection(this, 'validateTableData');

        if (!tableName || !data) {
            throw new DatabaseError('Table name and data are required');
        }

        if (typeof data !== 'object' || Array.isArray(data)) {
            throw new DatabaseError('Data must be an object');
        }

        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        const missingColumns = meta.columns.filter(col => !(col in data));
        if (missingColumns.length > 0) {
            throw new DatabaseError(`Missing required columns: ${missingColumns.join(', ')}`);
        }

        const extraColumns = Object.keys(data).filter(col => !meta.columns.includes(col));
        if (extraColumns.length > 0) {
            throw new DatabaseError(`Unknown columns: ${extraColumns.join(', ')}`);
        }

        for (const [column, rules] of Object.entries(meta.validations)) {
            const value = data[column];

            if (value === null || value === undefined) {
                throw new DatabaseError(`Column ${column} cannot be null or undefined`);
            }

            if (rules.type) {
                const actualType = typeof value;
                if (actualType !== rules.type) {
                    throw new DatabaseError(`Invalid type for column ${column}: expected ${rules.type}, got ${actualType}`);
                }
            }

            if (rules.type === 'number') {
                if (isNaN(value)) {
                    throw new DatabaseError(`Invalid numeric value for column ${column}`);
                }
                if (rules.min !== undefined && value < rules.min) {
                    throw new DatabaseError(`Value for column ${column} is less than minimum: ${rules.min}`);
                }
                if (rules.max !== undefined && value > rules.max) {
                    throw new DatabaseError(`Value for column ${column} is greater than maximum: ${rules.max}`);
                }
            }

            if (rules.pattern && rules.type === 'string') {
                const pattern = new RegExp(rules.pattern);
                if (!pattern.test(String(value))) {
                    throw new DatabaseError(`Value for column ${column} does not match pattern: ${rules.pattern}`);
                }
            }

            if (rules.enum && !rules.enum.includes(value)) {
                throw new DatabaseError(`Invalid value for column ${column}. Must be one of: ${rules.enum.join(', ')}`);
            }
        }

        for (const [column, relation] of Object.entries(meta.relations)) {
            const value = data[column];
            if (value !== undefined) {
                const relatedValue = await this.get(`${relation.table}.${value}`);
                if (!relatedValue) {
                    throw new DatabaseError(`Related record not found in table ${relation.table} for value ${value}`);
                }
            }
        }

        return true;
    }

    async update(key, updates) {
        if (!key) throw new DatabaseError('Key is required');
        if (typeof key !== 'string') throw new DatabaseError('Key must be a string');
        if (!updates || typeof updates !== 'object' || Array.isArray(updates)) {
            throw new DatabaseError('Updates must be a non-array object');
        }

        const [tableName, rowId] = key.split('.');

        if (rowId && tableName) {
            if (!await this.tableExists(tableName)) {
                throw new DatabaseError(`Table '${tableName}' does not exist`);
            }

            const currentData = await this.get(`${tableName}.${rowId}`);
            if (!currentData) {
                throw new DatabaseError(`Record not found: ${key}`);
            }

            const updatedData = { ...currentData };
            for (const [field, value] of Object.entries(updates)) {
                if (value === undefined) continue;
                updatedData[field] = value;
            }

            await this.validateTableData(tableName, updatedData);

            return this.set(`${tableName}.${rowId}`, updatedData);
        }

        const currentValue = await this.get(key);
        if (currentValue === null) {
            throw new DatabaseError(`Key not found: ${key}`);
        }
        if (typeof currentValue !== 'object' || Array.isArray(currentValue)) {
            throw new DatabaseError('Can only update object values');
        }

        const updatedValue = { ...currentValue };
        for (const [field, value] of Object.entries(updates)) {
            if (value === undefined) continue;
            updatedValue[field] = value;
        }

        return this.set(key, updatedValue);
    }

    async query(tableName, { where = {}, orderBy = null, limit = null, offset = 0 } = {}) {
        await this.ensureInitialized();
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        let results = [];
        const table = await this.getTable(tableName);

        for (const [id, row] of Object.entries(table)) {
            let matches = true;
            for (const [column, condition] of Object.entries(where)) {
                if (typeof condition === 'object') {
                    if (condition.$eq !== undefined && row[column] !== condition.$eq) matches = false;
                    if (condition.$ne !== undefined && row[column] === condition.$ne) matches = false;
                    if (condition.$gt !== undefined && row[column] <= condition.$gt) matches = false;
                    if (condition.$gte !== undefined && row[column] < condition.$gte) matches = false;
                    if (condition.$lt !== undefined && row[column] >= condition.$lt) matches = false;
                    if (condition.$lte !== undefined && row[column] > condition.$lte) matches = false;
                    if (condition.$in !== undefined && !condition.$in.includes(row[column])) matches = false;
                    if (condition.$nin !== undefined && condition.$nin.includes(row[column])) matches = false;
                } else {
                    if (row[column] !== condition) matches = false;
                }
            }
            if (matches) {
                results.push({ id, ...row });
            }
        }

        if (orderBy) {
            const [column, direction = 'asc'] = orderBy.split(' ');
            results.sort((a, b) => {
                if (direction === 'desc') {
                    return b[column] < a[column] ? -1 : b[column] > a[column] ? 1 : 0;
                }
                return a[column] < b[column] ? -1 : a[column] > b[column] ? 1 : 0;
            });
        }

        if (offset) results = results.slice(offset);
        if (limit) results = results.slice(0, limit);

        return results;
    }

    async findById(tableName, id) {
        return this.get(`${tableName}.${id}`);
    }

    async findOne(tableName, where = {}) {
        const results = await this.query(tableName, { where, limit: 1 });
        return results[0] || null;
    }

    async count(tableName, where = {}) {
        const results = await this.query(tableName, { where });
        return results.length;
    }

    async getTable(tableName) {
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        const table = { ...this.data[tableName] };
        delete table._meta;

        const decryptedTable = {};
        for (const [rowId, row] of Object.entries(table)) {
            const decryptedRow = {};
            for (const [field, val] of Object.entries(row)) {
                decryptedRow[field] = await this.decryptValue(val);
            }
            decryptedTable[rowId] = decryptedRow;
        }

        return decryptedTable;
    }

    async listTables() {
        const tables = [];
        for (const key of this.keys()) {
            const meta = this.data[key]?._meta;
            if (meta) {
                tables.push({
                    name: key,
                    columns: meta.columns,
                    rowCount: meta.rowCount,
                    created: meta.created
                });
            }
        }
        return tables;
    }

    async dropTable(tableName) {
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        delete this.data[tableName];
        if (this.autoSave) await this.save();
        return true;
    }

    async tableExists(tableName) {
        return !!(await this.get(`${tableName}._meta`));
    }

    async ensureInitialized() {
        if (this.initialized) {
            await this.initialized;
        }
    }

    async generateSQLContent(data = this.data) {
        let sql = '';
        const escapeSQLValue = (value) => {
            if (value === null || value === undefined) return 'NULL';
            if (typeof value === 'number') return value.toString();
            if (typeof value === 'boolean') return value ? '1' : '0';
            return `'${String(value).replace(/'/g, "''").replace(/\\/g, "\\\\")}'`;
        };

        const validateIdentifier = (identifier) => {
            if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(identifier)) {
                throw new DatabaseError(`Invalid SQL identifier: ${identifier}`);
            }
            return identifier;
        };

        for (const [tableName, tableData] of Object.entries(data)) {
            if (!tableData || typeof tableData !== 'object') continue;
            if (!tableData._meta) continue;

            const { columns = [], validations = {}, indexes = [] } = tableData._meta;
            const safeTableName = validateIdentifier(tableName);

            sql += `CREATE TABLE IF NOT EXISTS ${safeTableName} (\n`;
            sql += `  id VARCHAR(255) PRIMARY KEY,\n`;
            sql += columns.map(col => {
                const safeColName = validateIdentifier(col);
                const validation = validations[col] || {};
                let type = 'TEXT';
                if (validation.type === 'number') type = 'NUMERIC';
                if (validation.type === 'boolean') type = 'BOOLEAN';
                return `  ${safeColName} ${type}`;
            }).join(',\n');
            sql += '\n);\n\n';

            for (const index of indexes) {
                const safeIndexName = validateIdentifier(index);
                sql += `CREATE INDEX IF NOT EXISTS idx_${safeTableName}_${safeIndexName} `;
                sql += `ON ${safeTableName}(${safeIndexName});\n`;
            }
            sql += '\n';

            for (const [id, row] of Object.entries(tableData)) {
                if (id === '_meta') continue;
                const columns = Object.keys(row);
                const values = Object.values(row).map(escapeSQLValue);
                sql += `INSERT INTO ${safeTableName} (id, ${columns.join(', ')}) `;
                sql += `VALUES (${escapeSQLValue(id)}, ${values.join(', ')});\n`;
            }
            sql += '\n';
        }

        return sql;
    }

    startAutoBackup() {
        if (this.backupIntervalId) {
            clearInterval(this.backupIntervalId);
        }
        this.backupIntervalId = setInterval(() => {
            this.backup().catch(error => {
                this.debug && DatabaseError.info('Auto backup failed: ' + error.message);
            });
        }, this.backupIntervalTime);
    }

    stopAutoBackup() {
        if (this.backupIntervalId) {
            clearInterval(this.backupIntervalId);
            this.backupIntervalId = null;
        }
    }

    async select(tableName, { columns = ['*'], where = {}, orderBy = null, limit = null, offset = 0 } = {}) {
        await this.ensureInitialized();
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        let results = await this.query(tableName, { where, orderBy, limit, offset });

        if (columns[0] !== '*') {
            results = results.map(row => {
                const selectedColumns = {};
                columns.forEach(col => {
                    if (row.hasOwnProperty(col)) {
                        selectedColumns[col] = row[col];
                    }
                });
                return selectedColumns;
            });
        }

        return results;
    }

    async insert(tableName, data) {
        await this.ensureInitialized();
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        const id = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        await this.validateTableData(tableName, data);
        await this.set(`${tableName}.${id}`, data);

        return { id, ...data };
    }

    async insertMany(tableName, dataArray) {
        if (!Array.isArray(dataArray)) {
            throw new DatabaseError('Data must be an array');
        }

        const results = [];
        for (const data of dataArray) {
            const result = await this.insert(tableName, data);
            results.push(result);
        }

        return results;
    }

    async updateMany(tableName, where = {}, updates) {
        const results = await this.query(tableName, { where });
        const updatedRows = [];

        for (const row of results) {
            const updated = await this.update(`${tableName}.${row.id}`, updates);
            updatedRows.push(updated);
        }

        return updatedRows;
    }

    async delete(tableName, where = {}) {
        if (typeof tableName === 'string' && !where) {
            return super.delete(tableName);
        }

        const results = await this.query(tableName, { where });
        const deletedCount = results.length;

        for (const row of results) {
            await this.delete(`${tableName}.${row.id}`);
        }

        return { deletedCount };
    }

    async bulkDelete(tableName, where = {}) {
        const results = await this.query(tableName, { where });
        const deletedIds = results.map(row => row.id);
        const deletedCount = deletedIds.length;

        await Promise.all(deletedIds.map(id => super.delete(`${tableName}.${id}`)));

        return { deletedCount, deletedIds };
    }

    async truncate(tableName) {
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        const backup = { ...this.data[tableName] };
        await this.delete(tableName);
        this.data[tableName] = { _meta: backup._meta };
        if (this.autoSave) await this.save();

        return true;
    }

    async join(mainTable, joinTable, { on, type = 'INNER', select = ['*'] } = {}) {
        if (!on || !on.from || !on.to) {
            throw new DatabaseError('Join conditions must specify "from" and "to" columns');
        }

        const mainData = await this.getTable(mainTable);
        const joinData = await this.getTable(joinTable);

        const results = [];
        for (const [mainId, mainRow] of Object.entries(mainData)) {
            const matches = Object.entries(joinData).filter(([_, joinRow]) => 
                mainRow[on.from] === joinRow[on.to]
            );

            if (matches.length > 0) {
                matches.forEach(([joinId, joinRow]) => {
                    const merged = {
                        [`${mainTable}_id`]: mainId,
                        [`${joinTable}_id`]: joinId,
                        ...mainRow,
                        ...Object.fromEntries(
                            Object.entries(joinRow).map(([k, v]) => [`${joinTable}_${k}`, v])
                        )
                    };

                    if (select[0] !== '*') {
                        const selected = {};
                        select.forEach(col => {
                            if (merged.hasOwnProperty(col)) {
                                selected[col] = merged[col];
                            }
                        });
                        results.push(selected);
                    } else {
                        results.push(merged);
                    }
                });
            } else if (type === 'LEFT' || type === 'OUTER') {
                const merged = {
                    [`${mainTable}_id`]: mainId,
                    [`${joinTable}_id`]: null,
                    ...mainRow,
                    ...Object.fromEntries(
                        Object.keys(joinData[Object.keys(joinData)[0]] || {})
                            .map(k => [`${joinTable}_${k}`, null])
                    )
                };

                if (select[0] !== '*') {
                    const selected = {};
                    select.forEach(col => {
                        if (merged.hasOwnProperty(col)) {
                            selected[col] = merged[col];
                        }
                    });
                    results.push(selected);
                } else {
                    results.push(merged);
                }
            }
        }

        return results;
    }

    async describe(tableName) {
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        return {
            tableName,
            columns: meta.columns.map(col => ({
                name: col,
                type: meta.validations[col]?.type || 'string',
                constraints: meta.validations[col] || {},
                indexed: meta.indexes.includes(col)
            })),
            relations: meta.relations || {},
            created: meta.created,
            rowCount: meta.rowCount
        };
    }

    async showTables() {
        return this.listTables();
    }

    async alterTable(tableName, { addColumns = [], dropColumns = [], modifyValidations = {}, addIndexes = [], dropIndexes = [] } = {}) {
        const meta = this.data[tableName]?._meta;
        if (!meta) throw new DatabaseError(`Table '${tableName}' does not exist`);

        for (const column of addColumns) {
            if (meta.columns.includes(column)) {
                throw new DatabaseError(`Column '${column}' already exists in table '${tableName}'`);
            }
            if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(column)) {
                throw new DatabaseError(`Invalid column name: ${column}`);
            }
            meta.columns.push(column);
        }

        for (const column of dropColumns) {
            if (!meta.columns.includes(column)) {
                throw new DatabaseError(`Column '${column}' does not exist in table '${tableName}'`);
            }
            meta.columns = meta.columns.filter(col => col !== column);
            delete meta.validations[column];
            meta.indexes = meta.indexes.filter(idx => idx !== column);

            const table = this.data[tableName];
            for (const key in table) {
                if (key !== '_meta' && table[key]) {
                    delete table[key][column];
                }
            }
        }

        for (const [column, rules] of Object.entries(modifyValidations)) {
            if (!meta.columns.includes(column)) {
                throw new DatabaseError(`Column '${column}' does not exist in table '${tableName}'`);
            }
            meta.validations[column] = rules;
        }

        for (const column of addIndexes) {
            if (!meta.columns.includes(column)) {
                throw new DatabaseError(`Column '${column}' does not exist in table '${tableName}'`);
            }
            if (!meta.indexes.includes(column)) {
                meta.indexes.push(column);
            }
        }

        for (const column of dropIndexes) {
            meta.indexes = meta.indexes.filter(idx => idx !== column);
        }

        if (this.autoSave) await this.save();
        return true;
    }

    async groupBy(tableName, { columns = [], where = {}, having = null } = {}) {
        if (!columns.length) {
            throw new DatabaseError('At least one column must be specified for GROUP BY');
        }

        const results = await this.query(tableName, { where });
        const groups = new Map();

        for (const row of results) {
            const groupKey = columns.map(col => row[col]).join('|');
            if (!groups.has(groupKey)) {
                groups.set(groupKey, []);
            }
            groups.get(groupKey).push(row);
        }

        let groupedResults = Array.from(groups.entries()).map(([key, rows]) => {
            const groupValues = {};
            columns.forEach((col, index) => {
                groupValues[col] = rows[0][col];
            });

            groupValues.count = rows.length;
            
            const numericFields = Object.keys(rows[0]).filter(
                key => !columns.includes(key) && typeof rows[0][key] === 'number'
            );

            for (const field of numericFields) {
                groupValues[`sum_${field}`] = rows.reduce((sum, row) => sum + (row[field] || 0), 0);
                groupValues[`avg_${field}`] = groupValues[`sum_${field}`] / rows.length;
                groupValues[`min_${field}`] = Math.min(...rows.map(row => row[field] || 0));
                groupValues[`max_${field}`] = Math.max(...rows.map(row => row[field] || 0));
            }

            return groupValues;
        });

        if (having) {
            groupedResults = groupedResults.filter(group => {
                for (const [field, condition] of Object.entries(having)) {
                    if (typeof condition === 'object') {
                        if (condition.$gt !== undefined && !(group[field] > condition.$gt)) return false;
                        if (condition.$gte !== undefined && !(group[field] >= condition.$gte)) return false;
                        if (condition.$lt !== undefined && !(group[field] < condition.$lt)) return false;
                        if (condition.$lte !== undefined && !(group[field] <= condition.$lte)) return false;
                        if (condition.$eq !== undefined && group[field] !== condition.$eq) return false;
                        if (condition.$ne !== undefined && group[field] === condition.$ne) return false;
                    } else if (group[field] !== condition) {
                        return false;
                    }
                }
                return true;
            });
        }

        return groupedResults;
    }

    async distinct(tableName, columns = [], where = {}) {
        if (!columns.length) {
            throw new DatabaseError('At least one column must be specified for DISTINCT');
        }

        const results = await this.query(tableName, { where });
        const uniqueSet = new Set();

        return results.filter(row => {
            const key = columns.map(col => row[col]).join('|');
            if (uniqueSet.has(key)) return false;
            uniqueSet.add(key);
            return true;
        }).map(row => {
            if (columns.length === 1) {
                return row[columns[0]];
            }
            const result = {};
            columns.forEach(col => {
                result[col] = row[col];
            });
            return result;
        });
    }

    async renameTable(oldName, newName) {
        if (!this.data[oldName]) {
            throw new DatabaseError(`Table '${oldName}' does not exist`);
        }
        if (this.data[newName]) {
            throw new DatabaseError(`Table '${newName}' already exists`);
        }
        if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(newName)) {
            throw new DatabaseError('Invalid table name. Use only letters, numbers, and underscores, starting with a letter');
        }

        this.data[newName] = this.data[oldName];
        delete this.data[oldName];

        if (this.autoSave) await this.save();
        return true;
    }

    async union(queries) {
        if (!Array.isArray(queries) || queries.length < 2) {
            throw new DatabaseError('Union requires at least two queries');
        }

        const results = new Set();
        for (const query of queries) {
            const { tableName, columns = ['*'], where = {} } = query;
            const queryResults = await this.select(tableName, { columns, where });
            queryResults.forEach(row => results.add(JSON.stringify(row)));
        }

        return Array.from(results).map(row => JSON.parse(row));
    }

    async unionAll(queries) {
        if (!Array.isArray(queries) || queries.length < 2) {
            throw new DatabaseError('Union ALL requires at least two queries');
        }

        const results = [];
        for (const query of queries) {
            const { tableName, columns = ['*'], where = {} } = query;
            const queryResults = await this.select(tableName, { columns, where });
            results.push(...queryResults);
        }

        return results;
    }

    async orderByMultiple(tableName, { columns = [], where = {}, limit = null, offset = 0 } = {}) {
        if (!Array.isArray(columns) || !columns.length) {
            throw new DatabaseError('At least one ordering column must be specified');
        }

        let results = await this.query(tableName, { where });

        results.sort((a, b) => {
            for (const { column, direction = 'asc' } of columns) {
                if (a[column] < b[column]) return direction === 'asc' ? -1 : 1;
                if (a[column] > b[column]) return direction === 'asc' ? 1 : -1;
            }
            return 0;
        });

        if (offset) results = results.slice(offset);
        if (limit) results = results.slice(0, limit);

        return results;
    }

    async between(tableName, column, start, end, { inclusive = true } = {}) {
        if (!column) throw new DatabaseError('Column name is required');
        
        const where = {};
        if (inclusive) {
            where[column] = { $gte: start, $lte: end };
        } else {
            where[column] = { $gt: start, $lt: end };
        }

        return this.query(tableName, { where });
    }

    async like(tableName, column, pattern) {
        if (!column) throw new DatabaseError('Column name is required');
        if (!pattern) throw new DatabaseError('Pattern is required');

        const results = await this.query(tableName, {});
        const regexPattern = pattern
            .replace(/%/g, '.*')
            .replace(/_/g, '.');

        return results.filter(row => {
            const value = String(row[column] || '');
            return new RegExp(`^${regexPattern}$`, 'i').test(value);
        });
    }

    async in(tableName, column, values) {
        if (!column) throw new DatabaseError('Column name is required');
        if (!Array.isArray(values)) throw new DatabaseError('Values must be an array');

        const where = {
            [column]: { $in: values }
        };

        return this.query(tableName, { where });
    }

    async notIn(tableName, column, values) {
        if (!column) throw new DatabaseError('Column name is required');
        if (!Array.isArray(values)) throw new DatabaseError('Values must be an array');

        const where = {
            [column]: { $nin: values }
        };

        return this.query(tableName, { where });
    }

    async aggregate(tableName, { functions = [], where = {} } = {}) {
        const results = await this.query(tableName, { where });
        const aggregations = {};

        for (const func of functions) {
            const { name, column, alias } = func;
            const resultKey = alias || `${name}_${column}`;

            switch (name.toLowerCase()) {
                case 'sum':
                    aggregations[resultKey] = results.reduce((sum, row) => sum + (Number(row[column]) || 0), 0);
                    break;
                case 'avg':
                    aggregations[resultKey] = results.length ? 
                        results.reduce((sum, row) => sum + (Number(row[column]) || 0), 0) / results.length : 0;
                    break;
                case 'min':
                    aggregations[resultKey] = results.length ?
                        Math.min(...results.map(row => Number(row[column]) || 0)) : null;
                    break;
                case 'max':
                    aggregations[resultKey] = results.length ?
                        Math.max(...results.map(row => Number(row[column]) || 0)) : null;
                    break;
                case 'count':
                    aggregations[resultKey] = column === '*' ? results.length :
                        results.filter(row => row[column] !== null && row[column] !== undefined).length;
                    break;
            }
        }

        return aggregations;
    }

    async exists(tableName, where = {}) {
        const result = await this.findOne(tableName, where);
        return result !== null;
    }

    async findAndCount(tableName, { where = {}, orderBy = null, limit = null, offset = 0 } = {}) {
        const [rows, count] = await Promise.all([
            this.query(tableName, { where, orderBy, limit, offset }),
            this.count(tableName, where)
        ]);

        return { rows, count };
    }

    async bulkDelete(tableName, where = {}) {
        const results = await this.query(tableName, { where });
        const deletedIds = results.map(row => row.id);
        const deletedCount = deletedIds.length;

        await Promise.all(deletedIds.map(id => super.delete(`${tableName}.${id}`)));

        return { deletedCount, deletedIds };
    }

    async findByIds(tableName, ids) {
        if (!Array.isArray(ids)) {
            throw new DatabaseError('IDs must be an array');
        }

        const results = [];
        for (const id of ids) {
            const row = await this.findById(tableName, id);
            if (row) results.push({ id, ...row });
        }

        return results;
    }

    async upsert(tableName, data, uniqueColumns = []) {
        if (!uniqueColumns.length) {
            return this.insert(tableName, data);
        }

        const where = {};
        for (const col of uniqueColumns) {
            where[col] = data[col];
        }

        const existing = await this.findOne(tableName, where);
        if (existing) {
            await this.update(`${tableName}.${existing.id}`, data);
            return { id: existing.id, ...data, _upserted: 'updated' };
        }

        const result = await this.insert(tableName, data);
        return { ...result, _upserted: 'inserted' };
    }
}

protectClass(PulseaDB);

module.exports = PulseaDB;