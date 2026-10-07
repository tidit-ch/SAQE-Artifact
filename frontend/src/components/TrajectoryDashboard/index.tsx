import { useState, useCallback, useEffect, useRef } from "react";
import L from "leaflet";
import MapView from "./MapView";
import MapControl from "./MapControl/MapControl";
import ResizeBar from "./ResizeBar";
import JsonModal from "./JsonModal";
import { useTrajectoryList } from "../../hooks/useTrajectoryList";
import { GeometryType, SqlQueryResult } from "../../utils/types";
import { queryTrajectoryData } from "../../utils/mobilityDb";

export default function TrajectoryDashboard() {
	const [map, setMap] = useState<L.Map | null>(null);
	const [sidebarWidth, setSidebarWidth] = useState(33.333);
	const [modalEntity, setModalEntity] = useState<object | null>(null);
	const [drawnPolygon, setDrawnPolygon] = useState<string | null>(null);

	const trajectoryList = useTrajectoryList(map);
	const pushRef = useRef(trajectoryList.push);
	pushRef.current = trajectoryList.push;

	const handleWidthChange = useCallback(
		(w: number) => {
			setSidebarWidth(w);
			map?.invalidateSize();
		},
		[map],
	);

	const handleCollapse = useCallback(() => {
		setSidebarWidth(0);
		map?.invalidateSize();
	}, [map]);

	const handleExpand = useCallback(() => {
		setSidebarWidth(33.333);
		map?.invalidateSize();
	}, [map]);

	// Seed data on mount
	useEffect(() => {
		if (!map) return;
		const seed = async () => {
			try {
				const data = await queryTrajectoryData(
					"SELECT * FROM csv.public.porto_taxi LIMIT 15",
				);
				pushRef.current(data);
			} catch (error) {
				console.error(
					"Error loading seed data, using dummy data instead: ",
					error,
				);
				const dummyData: SqlQueryResult = {
					schema: { polyline: GeometryType.LINESTRING },
					data: [
						{
							polyline: [
								{ x: 51.505, y: -0.08, timestamp: "2023-10-01T12:00:00Z" },
								{ x: 51.5, y: -0.1, timestamp: "2023-10-01T12:00:15Z" },
								{ x: 51.51, y: -0.12, timestamp: "2023-10-01T12:00:30Z" },
							],
						},
					],
				};
				pushRef.current(dummyData);
			}
		};
		seed();
	}, [map]);

	return (
		<>
			<MapControl
				width={sidebarWidth}
				trajectoryList={trajectoryList}
				drawnPolygon={drawnPolygon}
				onShowEntityInfo={setModalEntity}
			/>
			<ResizeBar
				sidebarWidth={sidebarWidth}
				onWidthChange={handleWidthChange}
				onCollapse={handleCollapse}
				onExpand={handleExpand}
			/>
			<div className="order-1 md:order-3 h-[60vh] md:h-full max-h-[100vh] flex-1 min-w-[50%]">
				<MapView onMapReady={setMap} onPolygonDrawn={setDrawnPolygon} />
			</div>
			<JsonModal entity={modalEntity} onClose={() => setModalEntity(null)} />
		</>
	);
}
