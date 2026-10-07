import { useEffect, useRef, useState } from "react";
import JSONFormatter from "json-formatter-js";

interface JsonModalProps {
	entity: object | null;
	onClose: () => void;
}

export default function JsonModal({ entity, onClose }: JsonModalProps) {
	const contentRef = useRef<HTMLDivElement>(null);
	const [openDepth, setOpenDepth] = useState(1);

	useEffect(() => {
		if (!entity || !contentRef.current) return;
		contentRef.current.innerHTML = "";
		const formatter = new JSONFormatter(entity, openDepth, {
			hoverPreviewEnabled: true,
		});
		contentRef.current.appendChild(formatter.render());
	}, [entity, openDepth]);

	useEffect(() => {
		if (!entity) return;
		const handler = (e: KeyboardEvent) => {
			if (e.key === "Escape") onClose();
		};
		window.addEventListener("keydown", handler);
		return () => window.removeEventListener("keydown", handler);
	}, [entity, onClose]);

	if (!entity) return null;

	return (
		<div
			className="fixed inset-0 z-[9999] bg-slate-900/40 backdrop-blur-sm flex items-center justify-center p-4 md:p-12"
			onClick={(e) => {
				if (e.target === e.currentTarget) onClose();
			}}
		>
			<div className="bg-white rounded-lg shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col border border-slate-200">
				<div className="flex items-center justify-between px-4 h-11 border-b border-slate-200">
					<div className="flex items-center gap-2">
						<span className="material-symbols-outlined icon-sm text-ink-500">
							data_object
						</span>
						<span className="font-semibold text-[13px]">Entity data</span>
					</div>
					<div className="flex items-center gap-1.5">
						<button
							onClick={() => setOpenDepth(Infinity)}
							className="btn btn-ghost btn-xs"
						>
							Expand all
						</button>
						<button
							onClick={() => setOpenDepth(0)}
							className="btn btn-ghost btn-xs"
						>
							Collapse all
						</button>
						<div className="w-px h-5 bg-slate-200 mx-0.5" />
						<button
							onClick={onClose}
							className="icon-btn"
							title="Close (Esc)"
						>
							<span className="material-symbols-outlined icon-sm">close</span>
						</button>
					</div>
				</div>
				<div ref={contentRef} className="p-4 overflow-auto flex-1 font-mono text-[12px]" />
			</div>
		</div>
	);
}
