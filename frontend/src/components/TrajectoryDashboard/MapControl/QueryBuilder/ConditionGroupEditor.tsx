import type {
	ConditionGroup,
	ConditionNode,
	WhereCondition,
	ColumnInfo,
	UdfDef,
} from "../../../../utils/queryBuilderTypes";
import WhereConditionRow from "./WhereConditionRow";

let idCounter = 0;
const newId = () => `c${++idCounter}`;

interface Props {
	group: ConditionGroup;
	columns: ColumnInfo[];
	udfs: UdfDef[];
	drawnPolygon: string | null;
	isRoot?: boolean;
	onChange: (updated: ConditionGroup) => void;
	onRemove?: () => void;
}

export default function ConditionGroupEditor({
	group,
	columns,
	udfs,
	drawnPolygon,
	isRoot = false,
	onChange,
	onRemove,
}: Props) {
	const addLeaf = (kind: "simple" | "udf") => {
		const leaf: WhereCondition =
			kind === "simple"
				? { id: newId(), kind: "simple", column: "", operator: "=", value: "" }
				: { id: newId(), kind: "udf", udfName: "", args: [] };
		onChange({ ...group, children: [...group.children, leaf] });
	};

	const addSubGroup = () => {
		const sub: ConditionGroup = {
			id: newId(),
			kind: "group",
			logic: "AND",
			children: [],
		};
		onChange({ ...group, children: [...group.children, sub] });
	};

	const updateChild = (id: string, updated: ConditionNode) =>
		onChange({
			...group,
			children: group.children.map((c) => (c.id === id ? updated : c)),
		});

	const removeChild = (id: string) =>
		onChange({ ...group, children: group.children.filter((c) => c.id !== id) });

	const logicToggle = (
		<div className="seg">
			{(["AND", "OR"] as const).map((op) => (
				<button
					key={op}
					onClick={() => onChange({ ...group, logic: op })}
					data-active={group.logic === op}
					className="seg-btn"
				>
					{op}
				</button>
			))}
		</div>
	);

	const children = (
		<div className="flex flex-col gap-1.5">
			{group.children.map((child, index) => (
				<div key={child.id}>
					{index > 0 && (
						<div className="flex items-center gap-2 my-1">
							<div className="flex-1 border-t border-dashed border-slate-300" />
							<span className="text-2xs font-bold text-accent bg-accent-soft px-1.5 py-0.5 rounded shrink-0 tracking-wide">
								{group.logic}
							</span>
							<div className="flex-1 border-t border-dashed border-slate-300" />
						</div>
					)}

					{child.kind === "group" ? (
						<ConditionGroupEditor
							group={child}
							columns={columns}
							udfs={udfs}
							drawnPolygon={drawnPolygon}
							isRoot={false}
							onChange={(updated) => updateChild(child.id, updated)}
							onRemove={() => removeChild(child.id)}
						/>
					) : (
						<WhereConditionRow
							condition={child}
							columns={columns}
							udfs={udfs}
							drawnPolygon={drawnPolygon}
							onChange={(updated) => updateChild(child.id, updated)}
							onRemove={() => removeChild(child.id)}
						/>
					)}
				</div>
			))}

			{group.children.length === 0 && (
				<div className="rounded-md border border-dashed border-slate-300 bg-slate-50/40 px-3 py-3 text-center hint">
					No conditions yet. Add one below ↓
				</div>
			)}
		</div>
	);

	const addButtons = (
		<div className="flex items-center gap-1 mt-1.5">
			<button
				onClick={() => addLeaf("simple")}
				className="btn btn-secondary btn-sm flex-1"
			>
				<span className="material-symbols-outlined icon-sm">add</span>
				Column
			</button>
			<button
				onClick={() => addLeaf("udf")}
				className="btn btn-secondary btn-sm flex-1"
			>
				<span className="material-symbols-outlined icon-sm">function</span>
				Function
			</button>
			<button
				onClick={addSubGroup}
				className="btn btn-secondary btn-sm"
				title="Add nested group"
			>
				<span className="material-symbols-outlined icon-sm">
					account_tree
				</span>
				Group
			</button>
		</div>
	);

	if (isRoot) {
		return (
			<div className="flex flex-col gap-1.5">
				{group.children.length > 1 && (
					<div className="flex items-center gap-2 mb-0.5">
						<span className="hint">Combine with</span>
						{logicToggle}
					</div>
				)}
				{children}
				{addButtons}
			</div>
		);
	}

	return (
		<div className="well--accent">
			<div className="flex items-center justify-between mb-1.5">
				<div className="flex items-center gap-2">
					<span className="material-symbols-outlined icon-sm text-accent">
						account_tree
					</span>
					<span className="text-2xs font-semibold text-ink-700 uppercase tracking-wider">
						Group
					</span>
					{logicToggle}
				</div>
				{onRemove && (
					<button
						onClick={onRemove}
						className="icon-btn-inline"
						title="Remove group"
					>
						<span className="material-symbols-outlined icon-sm">close</span>
					</button>
				)}
			</div>
			{children}
			{addButtons}
		</div>
	);
}
