import { useEffect, useRef } from "react";
import L from "leaflet";
import "@geoman-io/leaflet-geoman-free";

interface MapViewProps {
	onMapReady: (map: L.Map) => void;
	onPolygonDrawn: (wkt: string) => void;
}

export default function MapView({ onMapReady, onPolygonDrawn }: MapViewProps) {
	const mapContainerRef = useRef<HTMLDivElement>(null);
	const mapRef = useRef<L.Map | null>(null);
	const onMapReadyRef = useRef(onMapReady);
	const onPolygonDrawnRef = useRef(onPolygonDrawn);
	onMapReadyRef.current = onMapReady;
	onPolygonDrawnRef.current = onPolygonDrawn;

	useEffect(() => {
		if (!mapContainerRef.current || mapRef.current) return;

		const map = L.map(mapContainerRef.current, {
			zoomControl: false, // we'll add it manually with a quieter position
		}).setView([51.505, -0.09], 13);

		L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
			attribution: "&copy; OpenStreetMap contributors",
			maxZoom: 19,
		}).addTo(map);

		L.control
			.zoom({
				position: "topleft",
			})
			.addTo(map);

		// Geoman: only the tools that make sense for selecting trajectory regions.
		// Hide the rest to keep the map chrome quiet.
		map.pm.addControls({
			position: "topleft",
			drawMarker: false,
			drawCircleMarker: false,
			drawPolyline: false,
			drawCircle: false,
			drawText: false,
			cutPolygon: false,
			rotateMode: false,
			drawRectangle: true,
			drawPolygon: true,
			editMode: true,
			dragMode: false,
			removalMode: true,
		});

		map.on("pm:create", (e: { layer: L.Layer }) => {
			const layer = e.layer as L.Polygon;
			const coordinates: L.LatLng[] =
				(layer.getLatLngs()[0] as L.LatLng[]) || [];
			const wkt = `POLYGON((${coordinates.map((c) => `${c.lat} ${c.lng}`).join(", ")}))`;
			onPolygonDrawnRef.current(wkt);
		});

		mapRef.current = map;
		onMapReadyRef.current(map);

		return () => {
			map.remove();
			mapRef.current = null;
		};
	}, []);

	return (
		<div
			ref={mapContainerRef}
			className="h-full min-h-full sticky bg-slate-100"
		/>
	);
}
