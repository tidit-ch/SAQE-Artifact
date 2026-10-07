import _ from "lodash";
import { EntityObject, SqlQueryResult } from "./types";
import type { TableInfo, ColumnInfo } from "./queryBuilderTypes";

const baseURL = _.get(
	import.meta.env,
	"VITE_BASE_URL",
	"http://localhost:3000",
);

export function processMobilityData(data: Array<EntityObject>) {
	const processedData = data.map((d: EntityObject) => {
		const date = new Date(d.timestamp * 1000); // Convert to milliseconds
		let tripDuration: number = 0;
		let polyline = _.cloneDeep(_.get(d, "polyline", []));
		delete d.polyline;
		polyline = polyline.map((p: L.LatLngExpression) => {
			if (Array.isArray(p)) {
				return p.reverse();
			}
		});
		d.polyline = { coordinates: polyline };
		if (polyline.length > 0) {
			tripDuration = (polyline.length - 1) * 15; // 15 seconds per point
		}
		return {
			...d,
			date,
			tripDuration,
		};
	});
	return processedData;
}

export async function fetchTables(): Promise<TableInfo[]> {
	try {
		const response = await fetch(`${baseURL}/datafusion/tables`);
		const data: Array<Record<string, string>> = await response.json();
		return data.map((row) => {
			const catalog = row["table_catalog"] ?? "";
			const schema = row["table_schema"] ?? "";
			const name = row["table_name"] ?? "";
			return {
				catalog,
				schema,
				name,
				fullName: `${catalog}.${schema}.${name}`,
			};
		});
	} catch {
		return [];
	}
}

export async function fetchTableColumns(
	fullTableName: string,
): Promise<ColumnInfo[]> {
	try {
		const response = await fetch(
			`${baseURL}/datafusion/table?table_name=${encodeURIComponent(fullTableName)}`,
		);
		const data: Array<Record<string, string>> = await response.json();
		return data.map((row) => ({
			// DataFusion DESCRIBE returns column_name / data_type; fall back to MySQL-style Field/Type
			name: row["column_name"] ?? row["Field"] ?? "",
			type: row["data_type"] ?? row["Type"] ?? "",
		}));
	} catch {
		return [];
	}
}

interface BackendUdfArg {
	name: string;
	description: string;
	data_type: string;
}

interface BackendUdfDetail {
	name: string;
	category: string;
	description: string;
	syntax_example: string;
	sql_example: string | null;
	arguments: BackendUdfArg[];
}

function inferParamType(
	dataType: string,
	argName: string,
	argDesc: string,
): import("./queryBuilderTypes").ParamType {
	const dt = dataType.toLowerCase();
	const name = argName.toLowerCase();
	const desc = argDesc.toLowerCase();
	if (dt.startsWith("list")) return "trajectory_column";
	if (dt.startsWith("struct")) return "column";
	if (dt.includes("timestamp")) return "timestamp_ms";
	if (["int64", "int32", "uint64", "uint32"].includes(dt)) return "integer";
	if (["float64", "float32"].includes(dt)) return "float";
	if (
		name.includes("polygon") ||
		desc.includes("wkt") ||
		desc.includes("polygon")
	)
		return "polygon_wkt";
	if (name.includes("strict")) return "strictness";
	if (name.includes("time_unit") || desc.includes("time unit"))
		return "time_unit";
	return "string";
}

export async function fetchUdfDetails(): Promise<
	import("./queryBuilderTypes").UdfDef[]
> {
	try {
		const response = await fetch(`${baseURL}/datafusion/udf_details`);
		const data: BackendUdfDetail[] = await response.json();
		return data.map((raw) => ({
			name: raw.name,
			category:
				raw.category as import("./queryBuilderTypes").UdfDef["category"],
			description: raw.description,
			syntax: raw.syntax_example,
			params: raw.arguments.map((arg) => ({
				name: arg.name,
				type: inferParamType(arg.data_type, arg.name, arg.description),
				description: arg.description,
			})),
			returnType: "",
			examples: raw.sql_example ? [raw.sql_example] : [],
		}));
	} catch {
		return [];
	}
}

export async function fetchCropFunctionDetails(): Promise<
	import("./queryBuilderTypes").CropFunctionDef[]
> {
	try {
		const response = await fetch(`${baseURL}/datafusion/crop_function_details`);
		const data: BackendUdfDetail[] = await response.json();
		return data.map((raw) => ({
			name: raw.name,
			description: raw.description,
			args: raw.arguments.map((arg) => ({
				name: arg.name,
				type: inferParamType(arg.data_type, arg.name, arg.description),
				description: arg.description,
			})),
		}));
	} catch {
		return [];
	}
}

export async function queryTrajectoryData(
	query: string,
	responseType: string = "json",
): Promise<SqlQueryResult> {
	try {
		const myHeaders = new Headers();
		myHeaders.append("Content-Type", "application/json");

		const raw = JSON.stringify({
			query,
			response_type: responseType,
		});

		const requestOptions: RequestInit = {
			method: "POST",
			headers: myHeaders,
			body: raw,
			redirect: "follow",
		};
		const response = await fetch(`${baseURL}/datafusion/query`, requestOptions);
		let data: SqlQueryResult = await response.json();
		return data;
	} catch (error) {
		console.error("Error fetching data: ", error);
		throw error;
	}
}
