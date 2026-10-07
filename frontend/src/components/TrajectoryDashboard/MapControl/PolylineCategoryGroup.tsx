import { useState } from "react";
import {
	TrajectoryGroupState,
	TrajectoryListAPI,
} from "../../../hooks/useTrajectoryList";
import PolylineItem from "./PolylineItem";
import { GeometryType } from "../../../utils/types";

interface PolylineCategoryGroupProps {
	groupIndex: number;
	group: TrajectoryGroupState;
	trajectoryList: TrajectoryListAPI;
	onShowEntityInfo: (entity: object) => void;
}

export default function PolylineCategoryGroup({
	groupIndex,
	group,
	trajectoryList,
	onShowEntityInfo,
}: PolylineCategoryGroupProps) {
	const [collapsed, setCollapsed] = useState(true);
	const typeLabel =
		group.geometryType === GeometryType.MULTILINESTRING
			? "MultiLineString"
			: "LineString";
	const typeBadgeClass =
		group.geometryType === GeometryType.MULTILINESTRING
			? "badge--purple"
			: "badge--blue";

	return (
		<div className="rounded-md border border-slate-200 overflow-hidden bg-white">
			<div className="flex flex-row items-center justify-between px-2 h-8 bg-slate-50/60 border-b border-slate-200">
				<button
					onClick={() => setCollapsed((c) => !c)}
					className="flex items-center gap-1.5 min-w-0 flex-1 text-left hover:text-ink-900"
				>
					<span className="material-symbols-outlined icon-sm text-ink-400 shrink-0">
						{collapsed ? "chevron_right" : "expand_more"}
					</span>
					<span className="font-mono text-[11.5px] font-semibold truncate text-ink-900">
						{group.fieldKey}
					</span>
					<span className={`badge ${typeBadgeClass}`}>{typeLabel}</span>
					<span className="text-2xs text-ink-400 shrink-0">
						{group.items.length}
					</span>
				</button>
				<button
					onClick={() => trajectoryList.toggleGroup(groupIndex)}
					className="icon-btn-inline icon-btn--accent"
					title={group.visible ? "Hide all on map" : "Show all on map"}
				>
					<span className="material-symbols-outlined icon-sm">
						{group.visible ? "visibility" : "visibility_off"}
					</span>
				</button>
			</div>
			{!collapsed && (
				<ul className="divide-y divide-slate-100">
					{group.items.map((item, itemIndex) => (
						<PolylineItem
							key={itemIndex}
							groupIndex={groupIndex}
							itemIndex={itemIndex}
							color={item.color}
							visible={item.visible}
							rawCoordinates={trajectoryList.getRawCoordinates(
								groupIndex,
								itemIndex,
							)}
							trajectoryList={trajectoryList}
							onShowInfo={() => {
								const entity = trajectoryList.getEntity(groupIndex, itemIndex);
								if (entity) onShowEntityInfo(entity);
							}}
						/>
					))}
				</ul>
			)}
		</div>
	);
}
