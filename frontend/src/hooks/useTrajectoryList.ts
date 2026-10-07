import { useState, useRef } from "react";
import L from "leaflet";
import _ from "lodash";
import { TrajectoryObject, GeometryType, SqlQueryResult } from "../utils/types";
import {
	generateRandomDarkHexColor,
	getLatLngArrayFromObject,
	getMultiLatLngArrayFromObject,
} from "../utils/utils";

export interface TrajectoryItem {
	entity: TrajectoryObject;
	color: string;
	visible: boolean;
}

export interface TrajectoryGroupState {
	fieldKey: string;
	geometryType: GeometryType;
	visible: boolean;
	items: TrajectoryItem[];
}

export interface DateTimeRange {
	min: Date;
	max: Date;
}

export interface TrajectoryListAPI {
	groups: TrajectoryGroupState[];
	push: (queryResult: SqlQueryResult) => void;
	clear: () => void;
	updateColor: (groupIdx: number, itemIdx: number, color: string) => void;
	toggleVisibility: (groupIdx: number, itemIdx: number) => void;
	toggleGroup: (groupIdx: number) => void;
	panTo: (groupIdx: number, itemIdx: number) => void;
	highlightOn: (groupIdx: number, itemIdx: number) => void;
	highlightOff: (groupIdx: number, itemIdx: number) => void;
	getEntity: (groupIdx: number, itemIdx: number) => TrajectoryObject | null;
	getDateTimeRange: () => DateTimeRange | null;
	getRawCoordinates: (groupIdx: number, itemIdx: number) => unknown;
}

export function useTrajectoryList(map: L.Map | null): TrajectoryListAPI {
	const [groups, setGroups] = useState<TrajectoryGroupState[]>([]);
	// polylineGroupsRef[groupIdx][itemIdx] = L.Polyline[] (segments for one trajectory)
	const polylineGroupsRef = useRef<L.Polyline[][][]>([]);
	const highlightMarkersRef = useRef<L.Marker[]>([]);

	function push(queryResult: SqlQueryResult) {
		if (!map) return;
		console.log("Received query result: ", queryResult);
		const { data: entities, schema } = queryResult;
		if (entities.length === 0) return;

		console.log("schema :: ", schema);

		let detectedFields: { fieldKey: string; geometryType: GeometryType }[] = [];
		Object.entries(schema).forEach(([fieldKey, fieldType]) => {
			if (
				fieldType == GeometryType.LINESTRING ||
				fieldType === GeometryType.MULTILINESTRING
			) {
				detectedFields.push({
					fieldKey,
					geometryType: fieldType,
				});
			}
		});

		console.log("Detected geometry fields: ", detectedFields);
		if (detectedFields.length === 0) {
			alert("No geometry fields detected in the query result");
			return;
		}

		console.log("Detected geometry fields: ", detectedFields);
		if (detectedFields.length === 0) {
			alert("No geometry fields detected in the query result");
			return;
		}

		const newGroups: TrajectoryGroupState[] = [];
		const newPolylineGroups: L.Polyline[][][] = [];
		let bounds: L.LatLngBounds | undefined;

		detectedFields.forEach(({ fieldKey, geometryType }) => {
			const groupItems: TrajectoryItem[] = [];
			const groupPolylines: L.Polyline[][] = [];

			entities.forEach((e) => {
				const color = generateRandomDarkHexColor();
				let polylineGroup: L.Polyline[];

				if (geometryType === GeometryType.MULTILINESTRING) {
					const multiCoords = getMultiLatLngArrayFromObject(e, fieldKey);
					polylineGroup = multiCoords.map((coords) =>
						L.polyline(coords, { color, weight: 5 }),
					);
				} else {
					const coords = getLatLngArrayFromObject(e, fieldKey);
					polylineGroup = [L.polyline(coords, { color, weight: 5 })];
				}

				if (polylineGroup.length === 0) return;

				groupItems.push({ entity: e, color, visible: true });
				groupPolylines.push(polylineGroup);

				polylineGroup.forEach((p) => {
					p.addTo(map);
					try {
						const b = p.getBounds();
						bounds = bounds ? bounds.extend(b) : b;
					} catch {
						// empty polyline, skip
					}
				});
			});

			if (groupItems.length > 0) {
				newGroups.push({
					fieldKey,
					geometryType,
					visible: true,
					items: groupItems,
				});
				newPolylineGroups.push(groupPolylines);
			}
		});

		if (newGroups.length > 0) {
			polylineGroupsRef.current = [
				...polylineGroupsRef.current,
				...newPolylineGroups,
			];
			setGroups((prev) => [...prev, ...newGroups]);
		}

		if (bounds) {
			map.fitBounds(bounds, { padding: [20, 20] });
			map.panTo(bounds.getCenter());
		}
	}

	function clear() {
		if (!map) return;
		polylineGroupsRef.current.forEach((groupPolylines) =>
			groupPolylines.forEach((itemPolylines) =>
				itemPolylines.forEach((p) => {
					if (map.hasLayer(p)) map.removeLayer(p);
				}),
			),
		);
		polylineGroupsRef.current = [];
		setGroups([]);
	}

	function updateColor(groupIdx: number, itemIdx: number, color: string) {
		if (!map) return;
		const itemPolylines = polylineGroupsRef.current[groupIdx]?.[itemIdx];
		if (!itemPolylines) return;
		itemPolylines.forEach((p) => p.setStyle({ color }));
		setGroups((prev) =>
			prev.map((g, gi) => {
				if (gi !== groupIdx) return g;
				return {
					...g,
					items: g.items.map((item, ii) =>
						ii === itemIdx ? { ...item, color } : item,
					),
				};
			}),
		);
	}

	function toggleVisibility(groupIdx: number, itemIdx: number) {
		if (!map) return;
		const itemPolylines = polylineGroupsRef.current[groupIdx]?.[itemIdx];
		if (!itemPolylines) return;

		setGroups((prev) =>
			prev.map((g, gi) => {
				if (gi !== groupIdx) return g;
				return {
					...g,
					items: g.items.map((item, ii) => {
						if (ii !== itemIdx) return item;
						const newVisible = !item.visible;
						if (newVisible) {
							itemPolylines.forEach((p) => p.addTo(map));
							try {
								let b = itemPolylines[0].getBounds();
								itemPolylines.forEach((p) => (b = b.extend(p.getBounds())));
								map.fitBounds(b, { padding: [20, 20] });
							} catch {
								// empty bounds
							}
						} else {
							itemPolylines.forEach((p) => {
								if (map.hasLayer(p)) map.removeLayer(p);
							});
						}
						return { ...item, visible: newVisible };
					}),
				};
			}),
		);
	}

	function toggleGroup(groupIdx: number) {
		if (!map) return;
		const groupPolylines = polylineGroupsRef.current[groupIdx];
		if (!groupPolylines) return;

		setGroups((prev) =>
			prev.map((g, gi) => {
				if (gi !== groupIdx) return g;
				const newVisible = !g.visible;
				groupPolylines.forEach((itemPolylines) =>
					itemPolylines.forEach((p) => {
						if (newVisible) {
							if (!map.hasLayer(p)) p.addTo(map);
						} else {
							if (map.hasLayer(p)) map.removeLayer(p);
						}
					}),
				);
				return {
					...g,
					visible: newVisible,
					items: g.items.map((item) => ({ ...item, visible: newVisible })),
				};
			}),
		);
	}

	function panTo(groupIdx: number, itemIdx: number) {
		if (!map) return;
		const itemPolylines = polylineGroupsRef.current[groupIdx]?.[itemIdx];
		if (!itemPolylines || !itemPolylines.some((p) => map.hasLayer(p))) {
			alert("Polyline is not visible, please make it visible first");
			return;
		}
		try {
			let bounds = itemPolylines[0].getBounds();
			itemPolylines.forEach((p) => (bounds = bounds.extend(p.getBounds())));
			map.fitBounds(bounds, { padding: [20, 20] });
			map.panTo(bounds.getCenter());
		} catch {
			// empty bounds
		}
	}

	function highlightOn(groupIdx: number, itemIdx: number) {
		if (!map) return;
		const itemPolylines = polylineGroupsRef.current[groupIdx]?.[itemIdx];
		if (!itemPolylines || !itemPolylines.some((p) => map.hasLayer(p))) return;

		const markers: L.Marker[] = [];
		itemPolylines.forEach((p) => {
			const coords = p.getLatLngs() as L.LatLng[];
			if (coords.length > 0) {
				markers.push(L.marker(coords[0]).addTo(map));
				markers.push(L.marker(coords[coords.length - 1]).addTo(map));
			}
			p.setStyle({ weight: 8 });
		});
		highlightMarkersRef.current = markers;

		itemPolylines.forEach((p) => {
			map.removeLayer(p);
			p.addTo(map);
		});
	}

	function highlightOff(groupIdx: number, itemIdx: number) {
		if (!map) return;
		highlightMarkersRef.current.forEach((m) => map.removeLayer(m));
		highlightMarkersRef.current = [];
		const itemPolylines = polylineGroupsRef.current[groupIdx]?.[itemIdx];
		if (itemPolylines) {
			itemPolylines.forEach((p) => p.setStyle({ weight: 5 }));
		}
	}

	function getEntity(
		groupIdx: number,
		itemIdx: number,
	): TrajectoryObject | null {
		return groups[groupIdx]?.items[itemIdx]?.entity ?? null;
	}

	function getDateTimeRange(): DateTimeRange | null {
		const allEntities = groups.flatMap((g) => g.items.map((i) => i.entity));
		if (allEntities.length === 0) return null;
		const sorted = [...allEntities].sort(
			(a, b) => _.get(a, "timestamp", 0) - _.get(b, "timestamp", 0),
		);
		const minTime = _.get(sorted[0], "timestamp", 0);
		const maxTime = _.get(sorted[sorted.length - 1], "timestamp", 0);
		if (!minTime || !maxTime) return null;
		return {
			min: new Date(minTime * 1000),
			max: new Date(maxTime * 1000),
		};
	}

	function getRawCoordinates(groupIdx: number, itemIdx: number): unknown {
		const group = groups[groupIdx];
		if (!group) return [];
		return _.get(group.items[itemIdx]?.entity, group.fieldKey, []);
	}

	return {
		groups,
		push,
		clear,
		updateColor,
		toggleVisibility,
		toggleGroup,
		panTo,
		highlightOn,
		highlightOff,
		getEntity,
		getDateTimeRange,
		getRawCoordinates,
	};
}
