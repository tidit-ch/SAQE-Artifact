import { default as TrajectoryDashboard } from "./components/TrajectoryDashboard";

export default function App() {
	return (
		<div className="flex flex-col md:flex-row h-screen max-h-[100vh] bg-surface-page text-ink-900">
			<TrajectoryDashboard />
		</div>
	);
}
