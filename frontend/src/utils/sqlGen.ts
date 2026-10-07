import type {
	QueryBuilderState,
	ConditionNode,
	WhereCondition,
	SimpleCondition,
	ParamType,
	UdfDef,
	CropFunctionDef,
	CropExprNode,
} from "./queryBuilderTypes";

function argToSql(value: string, paramType: ParamType): string {
	switch (paramType) {
		case "trajectory_column":
		case "column":
			return value;
		case "polygon_wkt":
		case "strictness":
		case "string":
		case "time_unit":
			return `'${value.replace(/'/g, "''")}'`;
		case "timestamp_ms": {
			const ms = value ? new Date(value).getTime() : 0;
			return isNaN(ms) ? "0" : String(ms);
		}
		case "integer":
		case "float":
			return value || "0";
	}
}

function leafToSql(
	condition: WhereCondition,
	udfs: UdfDef[],
): string | null {
	if (condition.kind === "simple") {
		const c = condition as SimpleCondition;
		if (!c.column || !c.operator) return null;
		if (c.operator === "IS NULL" || c.operator === "IS NOT NULL")
			return `${c.column} ${c.operator}`;
		if (!c.value) return null;
		return `${c.column} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
	}
	if (!condition.udfName) return null;
	const udfDef = udfs.find((u) => u.name === condition.udfName);
	const argsSql = condition.args.map((arg, i) =>
		argToSql(arg, udfDef?.params[i]?.type ?? "string"),
	);
	return `${condition.udfName}(${argsSql.join(", ")})`;
}

function nodeToSql(node: ConditionNode, udfs: UdfDef[]): string | null {
	if (node.kind === "group") {
		const parts = node.children
			.map((child) => nodeToSql(child, udfs))
			.filter((s): s is string => s !== null);
		if (parts.length === 0) return null;
		if (parts.length === 1) return parts[0];
		return `(${parts.join(` ${node.logic} `)})`;
	}
	return leafToSql(node, udfs);
}

function cropExprToSql(
	node: CropExprNode,
	cropFunctions: CropFunctionDef[],
	nested = false,
): string | null {
	if (node.kind === "call") {
		if (!node.functionName) return null;
		const funcDef = cropFunctions.find((f) => f.name === node.functionName);
		const argsSql = node.args.map((arg, i) =>
			argToSql(arg, funcDef?.args[i]?.type ?? "string"),
		);
		return `${node.functionName}(${argsSql.join(", ")})`;
	}
	const parts = node.children
		.map((child) => cropExprToSql(child, cropFunctions, true))
		.filter((s): s is string => s !== null);
	if (parts.length === 0) return null;
	if (parts.length === 1) return parts[0];
	const joined = parts.join(` ${node.logic} `);
	return nested ? `(${joined})` : joined;
}

export function generateSql(
	state: QueryBuilderState,
	udfs: UdfDef[],
	cropFunctions: CropFunctionDef[] = [],
): string {
	if (!state.table) return "";

	const cropExprs = (state.cropProjections ?? [])
		.filter((p) => p.linestringColumn && p.exprRoot.children.length > 0)
		.map((p) => {
			const exprSql = cropExprToSql(p.exprRoot, cropFunctions, false);
			if (!exprSql) return null;
			const expr = `crop(${p.linestringColumn}, ${exprSql})`;
			return p.alias ? `${expr} AS ${p.alias}` : expr;
		})
		.filter((s): s is string => s !== null);

	const regularCols =
		state.selectedColumns.length === 0 ? ["*"] : state.selectedColumns;
	const allSelectParts = [...regularCols, ...cropExprs];
	const selectClause = allSelectParts.join(", ");

	let sql = `SELECT ${selectClause}\nFROM ${state.table.fullName}`;

	// Root group: children joined without outer parens
	const rootParts = state.rootGroup.children
		.map((child) => nodeToSql(child, udfs))
		.filter((s): s is string => s !== null);

	if (rootParts.length > 0) {
		sql += `\nWHERE ${rootParts.join(`\n  ${state.rootGroup.logic} `)}`;
	}

	if (state.limit) {
		sql += `\nLIMIT ${state.limit}`;
	}

	return sql;
}
