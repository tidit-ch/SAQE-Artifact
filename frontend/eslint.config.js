import eslintPluginTs from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";

export default [
	{
		// Define the files to lint
		files: ["**/*.ts", "**/*.tsx", "**/*.js", "**/*.jsx"],
		// Specify the parser for TypeScript
		languageOptions: {
			parser: tsParser,
			sourceType: "module",
			ecmaVersion: "latest",
		},
		// Plugins to extend ESLint capabilities
		plugins: {
			"@typescript-eslint": eslintPluginTs,
		},

		// Enable recommended rules and Prettier integration
		rules: {
			...eslintPluginTs.configs.recommended.rules, // TypeScript recommended rules
			// "prettier/prettier": "error", // Enforce Prettier formatting
			semi: ["error", "always"], // Enforce semicolons
			"no-unused-vars": "warn", // Warn about unused variables
			"@typescript-eslint/no-explicit-any": "warn", // Warn about explicit any types
			"@typescript-eslint/no-this-alias": "warn", // Warn about using this as an alias
		},
	},
];
