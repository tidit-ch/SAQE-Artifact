import { useState, useEffect } from "react";
import { TrajectoryListAPI } from "../../../hooks/useTrajectoryList";
import { fetchUdfDetails } from "../../../utils/mobilityDb";
import type { UdfDef } from "../../../utils/queryBuilderTypes";
import QueryInput from "./QueryInput";
import DateTimeFilter from "./DateTimeFilter";
import SampleQueries from "./SampleQueries";
import DrawnPolygonInfo from "./DrawnPolygonInfo";
import PolylineList from "./PolylineList";
import RawResults from "./RawResults";
import UdfBrowser from "./QueryBuilder/UdfBrowser";
import Section from "./Section";

interface MapControlProps {
	width: number;
	trajectoryList: TrajectoryListAPI;
	drawnPolygon: string | null;
	onShowEntityInfo: (entity: object) => void;
}

export default function MapControl({
	width,
	trajectoryList,
	drawnPolygon,
	onShowEntityInfo,
}: MapControlProps) {
	const [udfs, setUdfs] = useState<UdfDef[]>([]);

	useEffect(() => {
		fetchUdfDetails().then(setUdfs);
	}, []);

	const dateTimeRange = trajectoryList.getDateTimeRange();
	const totalItems = trajectoryList.groups.reduce(
		(acc, g) => acc + g.items.length,
		0,
	);

	return (
		<aside
			className="order-2 md:order-1 h-[40vh] md:h-full max-h-[100vh] md:min-w-0 md:max-w-[50%] flex flex-col bg-surface-panel border-r border-slate-200"
			style={{ width: `${width}%` }}
		>
			{/* Sticky workspace header — app identity + primary tabs */}
			<header className="shrink-0 border-b border-slate-200 bg-white">
				<div className="flex items-center justify-between px-3 h-11">
					<div className="flex items-center gap-2">
						<div className="h-6 w-6 rounded bg-accent text-white flex items-center justify-center font-bold text-[11px] tracking-wide">
							S
						</div>
						<span className="font-semibold text-[13px] text-ink-900">SAQE</span>
						{/* <span className="badge badge--neutral">trajectory</span> */}
					</div>
				</div>
			</header>

			{/* Scrollable section list */}
			<div className="flex-1 overflow-y-auto">
				{/* Query input lives outside any collapsible wrapper — it's the primary surface */}
				<QueryInput
					trajectoryList={trajectoryList}
					drawnPolygon={drawnPolygon}
					udfs={udfs}
				/>

				<Section title="Time range" icon="schedule" defaultOpen={false}>
					<DateTimeFilter dateTimeRange={dateTimeRange} />
				</Section>

				{drawnPolygon && (
					<Section title="Drawn polygon" icon="hexagon" defaultOpen={true}>
						<DrawnPolygonInfo polygonWkt={drawnPolygon} />
					</Section>
				)}

				<Section
					title="Results"
					icon="route"
					defaultOpen={true}
					count={totalItems > 0 ? totalItems : undefined}
				>
					<PolylineList
						trajectoryList={trajectoryList}
						onShowEntityInfo={onShowEntityInfo}
					/>
				</Section>

				<Section title="Raw results" icon="data_object" defaultOpen={false}>
					<RawResults groups={trajectoryList.groups} />
				</Section>

				<Section
					title="Function reference"
					icon="menu_book"
					defaultOpen={false}
					count={udfs.length > 0 ? udfs.length : undefined}
				>
					<UdfBrowser registeredUdfs={udfs} />
				</Section>

				<Section title="Sample queries" icon="auto_awesome" defaultOpen={false}>
					<SampleQueries />
				</Section>
			</div>
		</aside>
	);
}
