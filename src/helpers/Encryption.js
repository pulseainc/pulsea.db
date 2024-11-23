const k9crypt = require('k9crypt');

class Encryption {
    constructor(secretKey) {
        if (!secretKey) {
            throw new Error('Secret key is required for encryption!');
        }
        this.encryptor = new k9crypt(secretKey);
    }

    async encrypt(data) {
        try {
            if (typeof data === 'object') {
                data = JSON.stringify(data);
            }
            return await this.encryptor.encrypt(data);
        } catch (error) {
            throw new Error(`Encryption failed: ${error.message}`);
        }
    }

    async decrypt(data) {
        try {
            const decrypted = await this.encryptor.decrypt(data);
            try {
                return JSON.parse(decrypted);
            } catch {
                return decrypted;
            }
        } catch (error) {
            throw new Error(`Decryption failed: ${error.message}`);
        }
    }
}

module.exports = Encryption;