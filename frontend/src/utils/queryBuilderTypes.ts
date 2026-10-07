export interface ColumnInfo {
	name: string;
	type: string;
}

export interface TableInfo {
	catalog: string;
	schema: string;
	name: string;
	fullName: string;
}

export type ParamType =
	| "trajectory_column"
	| "column"
	| "polygon_wkt"
	| "strictness"
	| "integer"
	| "float"
	| "timestamp_ms"
	| "time_unit"
	| "string";

export interface UdfParamDef {
	name: string;
	type: ParamType;
	description: string;
	defaultValue?: string;
}

export interface UdfDef {
	name: string;
	category:
		| "spatial"
		| "temporal"
		| "spatio_temporal"
		| "geodata"
		| "utility"
		| "map_matching";
	description: string;
	syntax: string;
	params: UdfParamDef[];
	returnType: string;
	examples?: string[];
}

export type ConditionOperator =
	| "="
	| "!="
	| ">"
	| ">="
	| "<"
	| "<="
	| "LIKE"
	| "IS NULL"
	| "IS NOT NULL";

export interface SimpleCondition {
	id: string;
	kind: "simple";
	column: string;
	operator: ConditionOperator;
	value: string;
}

export interface UdfCondition {
	id: string;
	kind: "udf";
	udfName: string;
	args: string[];
}

export type WhereCondition = SimpleCondition | UdfCondition;

export interface ConditionGroup {
	id: string;
	kind: "group";
	logic: "AND" | "OR";
	children: ConditionNode[];
}

export type ConditionNode = WhereCondition | ConditionGroup;

export interface CropFunctionArgDef {
	name: string;
	type: ParamType;
	description: string;
}

export interface CropFunctionDef {
	name: string;
	description: string;
	args: CropFunctionArgDef[];
}

export interface CropFunctionCall {
	id: string;
	kind: "call";
	functionName: string;
	args: string[];
}

export interface CropExprGroup {
	id: string;
	kind: "group";
	logic: "AND" | "OR";
	children: CropExprNode[];
}

export type CropExprNode = CropFunctionCall | CropExprGroup;

export interface CropProjection {
	id: string;
	linestringColumn: string;
	exprRoot: CropExprGroup;
	alias: string;
}

export interface QueryBuilderState {
	table: TableInfo | null;
	selectedColumns: string[];
	cropProjections: CropProjection[];
	rootGroup: ConditionGroup;
	limit: string;
}
