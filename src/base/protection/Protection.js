const names = new WeakMap();
const methods = new WeakMap();
const dbname = 'PulseaDB';

const safe = new Set([
    'encryptValue',
    'decryptValue',
    'validateTableData',
    'sanitizePath',
    'init'
]);

function lock(target) {
    safe.forEach(method => {
        const desc = Object.getOwnPropertyDescriptor(target.prototype, method);
        if (desc) {
            Object.defineProperty(target.prototype, method, {
                ...desc,
                configurable: false,
                writable: false
            });
        }
    });

    Object.defineProperty(target, 'name', {
        value: dbname,
        writable: false,
        configurable: false
    });

    Object.freeze(target);
    Object.freeze(target.prototype);
}

function setup(instance) {
    methods.set(instance, safe);
}

function verify(instance, methodName) {
    return methods.get(instance)?.has(methodName) ?? false;
}

function guard(instance, methodName) {
    if (!verify(instance, methodName)) {
        throw new Error('Critical method tampering detected');
    }
}

module.exports = {
    protectClass: lock,
    initializeProtection: setup,
    checkMethodProtection: guard,
    protectedMethods: safe
};