import { TrajectoryListAPI } from "../../../hooks/useTrajectoryList";
import PolylineCategoryGroup from "./PolylineCategoryGroup";

interface PolylineListProps {
	trajectoryList: TrajectoryListAPI;
	onShowEntityInfo: (entity: object) => void;
}

export default function PolylineList({
	trajectoryList,
	onShowEntityInfo,
}: PolylineListProps) {
	if (trajectoryList.groups.length === 0) {
		return (
			<div className="rounded-md border border-dashed border-slate-300 bg-slate-50/40 px-3 py-6 text-center">
				<span className="material-symbols-outlined text-ink-400 icon-md block mb-1">
					route
				</span>
				<p className="hint">No trajectories yet. Run a query to populate.</p>
			</div>
		);
	}

	return (
		<div className="flex flex-col gap-1.5">
			{trajectoryList.groups.map((group, groupIndex) => (
				<PolylineCategoryGroup
					key={`${group.fieldKey}-${group.geometryType}`}
					groupIndex={groupIndex}
					group={group}
					trajectoryList={trajectoryList}
					onShowEntityInfo={onShowEntityInfo}
				/>
			))}
		</div>
	);
}
