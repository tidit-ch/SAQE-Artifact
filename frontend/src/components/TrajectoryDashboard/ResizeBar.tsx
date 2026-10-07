import { useEffect, useRef } from "react";

interface ResizeBarProps {
	sidebarWidth: number;
	onWidthChange: (width: number) => void;
	onCollapse: () => void;
	onExpand: () => void;
}

export default function ResizeBar({
	sidebarWidth,
	onWidthChange,
	onCollapse,
	onExpand,
}: ResizeBarProps) {
	const barRef = useRef<HTMLDivElement>(null);
	const isResizingRef = useRef(false);
	const onWidthChangeRef = useRef(onWidthChange);
	onWidthChangeRef.current = onWidthChange;

	useEffect(() => {
		const handleMouseMove = (e: MouseEvent) => {
			if (!isResizingRef.current) return;
			const container = barRef.current?.parentElement;
			if (!container) return;
			const rect = container.getBoundingClientRect();
			let pct = ((e.clientX - rect.left) / rect.width) * 100;
			pct = Math.max(0, Math.min(50, pct));
			onWidthChangeRef.current(pct);
		};

		const handleMouseUp = () => {
			if (!isResizingRef.current) return;
			isResizingRef.current = false;
			document.body.style.cursor = "";
			document.body.style.userSelect = "";
		};

		document.addEventListener("mousemove", handleMouseMove);
		document.addEventListener("mouseup", handleMouseUp);

		return () => {
			document.removeEventListener("mousemove", handleMouseMove);
			document.removeEventListener("mouseup", handleMouseUp);
		};
	}, []);

	const handleMouseDown = (e: React.MouseEvent) => {
		if ((e.target as HTMLElement).closest("button")) return;
		e.preventDefault();
		isResizingRef.current = true;
		document.body.style.cursor = "col-resize";
		document.body.style.userSelect = "none";
	};

	return (
		<div
			ref={barRef}
			onMouseDown={handleMouseDown}
			className="group hidden md:flex md:order-2 relative items-center justify-center w-1 cursor-col-resize select-none flex-shrink-0 bg-slate-200 hover:bg-accent active:bg-accent-hover transition-colors"
		>
			{sidebarWidth <= 0 ? (
				<button
					onClick={onExpand}
					className="absolute z-50 left-0 top-1/2 -translate-y-1/2
						bg-white text-ink-700 hover:text-accent hover:bg-accent-soft
						border border-slate-200 rounded-r-md
						h-12 w-5 flex items-center justify-center
						shadow-sm transition-colors active:scale-95"
					title="Expand panel"
				>
					<span className="material-symbols-outlined icon-sm">
						chevron_right
					</span>
				</button>
			) : (
				<button
					onClick={onCollapse}
					className="absolute z-50 right-0 top-1/2 -translate-y-1/2
						bg-white text-ink-700 hover:text-accent hover:bg-accent-soft
						border border-slate-200 rounded-l-md
						h-12 w-5 flex items-center justify-center
						shadow-sm transition-all active:scale-95
						opacity-0 group-hover:opacity-100"
					title="Collapse panel"
				>
					<span className="material-symbols-outlined icon-sm">
						chevron_left
					</span>
				</button>
			)}
		</div>
	);
}
