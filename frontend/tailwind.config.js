/** @type {import('tailwindcss').Config} */
export default {
	content: ["./index.html", "./src/**/*.{html,js,ts,jsx,tsx}"],
	theme: {
		extend: {
			fontFamily: {
				sans: [
					"InterVariable",
					"Inter",
					"system-ui",
					"-apple-system",
					"Segoe UI",
					"Roboto",
					"sans-serif",
				],
				mono: [
					"JetBrains Mono",
					"ui-monospace",
					"SFMono-Regular",
					"Menlo",
					"Consolas",
					"monospace",
				],
			},
			fontSize: {
				"2xs": ["10px", { lineHeight: "14px" }],
			},
			boxShadow: {
				xs: "0 1px 2px 0 rgb(15 23 42 / 0.04)",
				panel: "0 1px 2px 0 rgb(15 23 42 / 0.04), 0 0 0 1px rgb(15 23 42 / 0.04)",
			},
			colors: {
				ink: {
					900: "#0f172a",
					700: "#334155",
					500: "#64748b",
					400: "#94a3b8",
				},
				surface: {
					page: "#f8fafc",
					panel: "#ffffff",
					sunken: "#f1f5f9",
					code: "#0f172a",
				},
				accent: {
					DEFAULT: "#2563eb",
					hover: "#1d4ed8",
					soft: "#eff6ff",
				},
			},
		},
	},
	plugins: [],
};
