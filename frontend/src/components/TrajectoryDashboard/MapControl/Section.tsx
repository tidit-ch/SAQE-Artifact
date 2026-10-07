import { ReactNode, useState } from "react";

interface SectionProps {
	title: string;
	icon?: string;
	defaultOpen?: boolean;
	count?: number;
	right?: ReactNode;
	children: ReactNode;
}

/**
 * A collapsible section for the left sidebar.
 *
 * Visually: a compact header bar with an uppercase label and an optional
 * right-side adornment (count badge, action buttons), and a body that is
 * mounted only when open. Designed to live inside a parent that uses
 * `divide-y` or stacks `.section` children.
 */
export default function Section({
	title,
	icon,
	defaultOpen = true,
	count,
	right,
	children,
}: SectionProps) {
	const [open, setOpen] = useState(defaultOpen);

	return (
		<div className="section">
			<div
				role="button"
				tabIndex={0}
				onClick={() => setOpen((o) => !o)}
				onKeyDown={(e) => {
					if (e.key === "Enter" || e.key === " ") {
						e.preventDefault();
						setOpen((o) => !o);
					}
				}}
				className="section-header"
			>
				<div className="flex items-center gap-2 min-w-0">
					{icon && (
						<span className="material-symbols-outlined icon-sm text-ink-400">
							{icon}
						</span>
					)}
					<span className="truncate">{title}</span>
					{count !== undefined && (
						<span className="badge badge--neutral">{count}</span>
					)}
				</div>
				<div className="flex items-center gap-1">
					{right && <div onClick={(e) => e.stopPropagation()}>{right}</div>}
					<span
						className="material-symbols-outlined chevron"
						style={{ transform: open ? "rotate(0deg)" : "rotate(-90deg)" }}
					>
						expand_more
					</span>
				</div>
			</div>
			{open && <div className="section-body">{children}</div>}
		</div>
	);
}
