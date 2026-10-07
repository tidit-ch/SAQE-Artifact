export type EntityObject = { [key: string]: any };

export type SpatioTemporalPoint = {
	x: number;
	y: number;
	m: number;
};

export type TrajectoryObject = {
	[key: string]: any;
};

export type SqlQueryResult = {
	data: Array<TrajectoryObject>;
	schema: {
		[key: string]: string;
	};
};

export enum GeometryType {
	LINESTRING = "geoarrow.linestring",
	MULTILINESTRING = "geoarrow.multilinestring",
}
