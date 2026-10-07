import { LatLngExpression, LatLng } from "leaflet";
import { TrajectoryObject, SpatioTemporalPoint } from "./types";

export type PolylineList = Array<Array<LatLngExpression>>;

export function generateRandomDarkHexColor() {
	const r = Math.floor(Math.random() * 160);
	const g = Math.floor(Math.random() * 160);
	const b = Math.floor(Math.random() * 160);

	return `#${r.toString(16).padStart(2, "0")}${g
		.toString(16)
		.padStart(2, "0")}${b.toString(16).padStart(2, "0")}`;
}

export function getLatLngArrayFromObject(
	trajectoryObject: TrajectoryObject,
	fieldKey: string = "polyline",
): Array<LatLngExpression> {
	return (trajectoryObject[fieldKey] || []).map(
		(point: SpatioTemporalPoint) => new LatLng(point.y, point.x),
	);
}

export function getMultiLatLngArrayFromObject(
	trajectoryObject: TrajectoryObject,
	fieldKey: string,
): Array<Array<LatLngExpression>> {
	return (trajectoryObject[fieldKey] || []).map(
		(line: Array<SpatioTemporalPoint>) =>
			line.map((point: SpatioTemporalPoint) => new LatLng(point.y, point.x)),
	);
}
