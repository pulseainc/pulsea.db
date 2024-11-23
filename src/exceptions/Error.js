class DatabaseError extends Error {
    constructor(message) {
        const RED = '\x1b[31m';
        const RESET = '\x1b[0m';
        const BOLD = '\x1b[1m';
        const formattedMessage = `${RED}${BOLD}[PULSEA] ${message}${RESET}`;
        
        super(formattedMessage);
        this.name = 'DatabaseError';
        
        if (this.stack) {
            this.stack = this.stack.split('\n').map((line, index) => {
                if (index === 0) return line;
                return `${RED}${line}${RESET}`;
            }).join('\n');
        }
    }

    static info(message) {
        const BLUE = '\x1b[34m';
        const RESET = '\x1b[0m';
        const BOLD = '\x1b[1m';
        console.log(`${BLUE}${BOLD}[PULSEA] ${message}${RESET}`);
    }
}

module.exports = { DatabaseError };