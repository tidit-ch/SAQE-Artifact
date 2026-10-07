import { TrajectoryListAPI } from "../../../hooks/useTrajectoryList";

interface PolylineItemProps {
	groupIndex: number;
	itemIndex: number;
	color: string;
	visible: boolean;
	rawCoordinates: unknown;
	trajectoryList: TrajectoryListAPI;
	onShowInfo: () => void;
}

export default function PolylineItem({
	groupIndex,
	itemIndex,
	color,
	visible,
	rawCoordinates,
	trajectoryList,
	onShowInfo,
}: PolylineItemProps) {
	const polylineString = JSON.stringify(rawCoordinates);

	return (
		<li
			className={`flex items-center gap-1.5 px-2 py-1 hover:bg-slate-50 transition-colors ${
				visible ? "" : "opacity-50"
			}`}
			onMouseEnter={() => trajectoryList.highlightOn(groupIndex, itemIndex)}
			onMouseLeave={() => trajectoryList.highlightOff(groupIndex, itemIndex)}
		>
			{/* Color swatch + picker */}
			<label
				className="relative shrink-0 cursor-pointer"
				title="Change color"
			>
				<span
					className="block h-3 w-3 rounded-sm border border-slate-300 shadow-xs"
					style={{ backgroundColor: color }}
				/>
				<input
					type="color"
					value={color}
					disabled={!visible}
					onChange={(e) =>
						trajectoryList.updateColor(groupIndex, itemIndex, e.target.value)
					}
					className="absolute inset-0 opacity-0 cursor-pointer"
				/>
			</label>

			{/* Index */}
			<span className="text-2xs font-mono text-ink-400 shrink-0 w-5 text-right">
				#{itemIndex + 1}
			</span>

			{/* Coordinates preview */}
			<span className="font-mono text-[11px] text-ink-500 truncate flex-1">
				{polylineString}
			</span>

			{/* Actions */}
			<div className="flex items-center gap-0.5 shrink-0">
				<button
					onClick={() => trajectoryList.panTo(groupIndex, itemIndex)}
					className="icon-btn-inline icon-btn--accent"
					title="Pan to trajectory"
				>
					<span className="material-symbols-outlined icon-sm">
						pin_drop
					</span>
				</button>
				<button
					onClick={onShowInfo}
					className="icon-btn-inline icon-btn--accent"
					title="Show entity data"
				>
					<span className="material-symbols-outlined icon-sm">info</span>
				</button>
				<button
					onClick={() =>
						trajectoryList.toggleVisibility(groupIndex, itemIndex)
					}
					className="icon-btn-inline icon-btn--accent"
					title={visible ? "Hide on map" : "Show on map"}
				>
					<span className="material-symbols-outlined icon-sm">
						{visible ? "visibility" : "visibility_off"}
					</span>
				</button>
			</div>
		</li>
	);
}
