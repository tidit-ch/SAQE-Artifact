import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import eslint from "vite-plugin-eslint";
import path from "path";
import { viteStaticCopy } from "vite-plugin-static-copy";

export default defineConfig({
	root: ".",
	server: {
		host: "0.0.0.0",
		port: 5173,
		watch: {
			usePolling: true,
		},
	},
	plugins: [
		react(),
		eslint({
			failOnWarning: false,
			failOnError: true,
			fix: false,
			overrideConfigFile: path.resolve(__dirname, "eslint.config.js"),
		}),
		viteStaticCopy({
			targets: [
				{
					src: "node_modules/leaflet/dist/images/*",
					dest: "./",
				},
			],
		}),
	],
});
