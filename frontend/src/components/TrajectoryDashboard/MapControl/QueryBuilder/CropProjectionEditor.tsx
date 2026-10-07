import type {
	CropProjection,
	CropFunctionDef,
	CropExprGroup,
	ColumnInfo,
} from "../../../../utils/queryBuilderTypes";
import CropExprEditor from "./CropExprEditor";

interface CropProjectionEditorProps {
	projections: CropProjection[];
	cropFunctions: CropFunctionDef[];
	columns: ColumnInfo[];
	drawnPolygon: string | null;
	onChange: (projections: CropProjection[]) => void;
}

function isLinestringColumn(col: ColumnInfo): boolean {
	return col.type.toLowerCase().startsWith("list");
}

const newId = () =>
	crypto.randomUUID?.() ??
	`${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

function newProjection(): CropProjection {
	const exprRoot: CropExprGroup = {
		id: newId(),
		kind: "group",
		logic: "OR",
		children: [],
	};
	return {
		id: newId(),
		linestringColumn: "",
		exprRoot,
		alias: "",
	};
}

export default function CropProjectionEditor({
	projections,
	cropFunctions,
	columns,
	drawnPolygon,
	onChange,
}: CropProjectionEditorProps) {
	const linestringColumns = columns.filter(isLinestringColumn);

	const update = (id: string, patch: Partial<CropProjection>) => {
		onChange(projections.map((p) => (p.id === id ? { ...p, ...patch } : p)));
	};

	const remove = (id: string) => {
		onChange(projections.filter((p) => p.id !== id));
	};

	return (
		<div className="flex flex-col gap-2">
			{projections.map((proj) => (
				<div key={proj.id} className="well">
					<div className="flex items-center gap-1.5 mb-2">
						<span className="material-symbols-outlined icon-sm text-accent">
							content_cut
						</span>
						<span className="text-[11px] font-semibold text-ink-700 flex-1">
							crop( … )
						</span>
						<button
							onClick={() => remove(proj.id)}
							className="icon-btn-inline"
							title="Remove"
						>
							<span className="material-symbols-outlined icon-sm">close</span>
						</button>
					</div>

					<div className="flex flex-col gap-2">
						{/* Linestring column */}
						<div className="flex flex-col gap-0.5">
							<label className="text-2xs font-medium text-ink-700">
								column
								<span className="ml-1 font-mono font-normal text-ink-400">
									linestring
								</span>
							</label>
							<select
								value={proj.linestringColumn}
								onChange={(e) =>
									update(proj.id, { linestringColumn: e.target.value })
								}
								className="select"
							>
								<option value="">— select column —</option>
								{(linestringColumns.length > 0 ? linestringColumns : columns).map(
									(c) => (
										<option key={c.name} value={c.name}>
											{c.name}
										</option>
									),
								)}
							</select>
						</div>

						{/* Expression tree */}
						<div className="flex flex-col gap-0.5">
							<label className="text-2xs font-medium text-ink-700">
								crop expression
							</label>
							<CropExprEditor
								group={proj.exprRoot}
								cropFunctions={cropFunctions}
								drawnPolygon={drawnPolygon}
								isRoot={true}
								onChange={(exprRoot) => update(proj.id, { exprRoot })}
							/>
						</div>

						{/* Alias */}
						<div className="flex flex-col gap-0.5">
							<label className="text-2xs font-medium text-ink-700">
								alias
								<span className="ml-1 font-normal text-ink-400">(optional)</span>
							</label>
							<input
								type="text"
								value={proj.alias}
								onChange={(e) => update(proj.id, { alias: e.target.value })}
								placeholder="e.g. cropped_trajectory"
								className="input font-mono text-[12px]"
							/>
						</div>
					</div>
				</div>
			))}

			<button
				onClick={() => onChange([...projections, newProjection()])}
				className="btn btn-secondary btn-xs self-start"
			>
				<span className="material-symbols-outlined icon-sm">add</span>
				Add crop projection
			</button>
		</div>
	);
}
