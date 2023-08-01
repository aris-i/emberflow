module.exports = {
    // Set the path to your TypeScript files
    roots: ['<rootDir>/'],

    // Add the .ts and .tsx file extensions
    moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],

    // Add ts-jest as a transform for TypeScript files
    transform: {
        '^.+\\.tsx?$': 'ts-jest',
    },

    // Add a moduleNameMapper for custom aliases
    moduleNameMapper: {
        '^@/(.*)$': '<rootDir>/src/$1',
        'firebase-functions/lib/v2/providers/database':
            '<rootDir>/node_modules/firebase-functions/lib/v2/providers/database.js',
    },

    // Add a testRegex to specify the location of your test files
    testRegex: '(/__tests__/.*|(\\.|/)(test|spec))\\.tsx?$',

    // Add the moduleDirectories to allow import modules without the relative path
    moduleDirectories: ['node_modules', 'src'],

    // Add a coverage threshold
    coverageThreshold: {
        global: {
            statements: 80,
            branches: 80,
            functions: 80,
            lines: 80,
        },
    },
};
