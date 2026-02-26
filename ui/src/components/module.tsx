"use client";

import { LucideIcon } from "lucide-react";

interface OrbitNode {
  icon: LucideIcon;
  label: string;
  onClick?: () => void;
}

interface ModuleProps {
  centerIcon?: LucideIcon;
  centerLabel?: string;
  orbitNodes: [OrbitNode, OrbitNode, OrbitNode];
  className?: string;
}

export function Module({
  centerIcon: CenterIcon,
  centerLabel,
  orbitNodes,
  className = "",
}: ModuleProps) {
  const fallbackCenterIcon = CenterIcon ?? orbitNodes[0]?.icon;
  const fallbackCenterLabel = centerLabel ?? orbitNodes[0]?.label;
  const CenterGraphic = fallbackCenterIcon;

  // Positions for 3 nodes evenly spaced at 0, 120, 240 degrees around inner circle
  const orbitPositions = [
    { angle: 0, tooltipPosition: "top" as const },
    { angle: 120, tooltipPosition: "bottom" as const },
    { angle: 240, tooltipPosition: "bottom" as const },
  ];

  // Radius for orbit nodes - placed between inner circle and outer dashed ring
  // Inner circle is ~46% size, orbit nodes ~20%, so ~36% keeps them between rings.
  const orbitRadius = 36;
  const orbitNodeSize = 20;

  return (
    <div
      className={`relative ${className}`}
      role="group"
      aria-label="Module navigation"
    >
      {/* Outer dotted ring with glow animation - outermost boundary */}
      <div className="absolute inset-0 rounded-full border border-dotted border-white/20 bg-white/8" />

      {/* Inner circle with icon and text */}
      <div className="absolute left-1/2 top-1/2 flex h-[46%] w-[46%] -translate-x-1/2 -translate-y-1/2 flex-col items-center justify-center rounded-full border border-white/25 bg-slate-800/70 shadow-lg">
        {CenterGraphic && <CenterGraphic className="h-[40%] w-[40%] text-white/90" />}
        {fallbackCenterLabel && (
          <span className="mt-[6%] px-[6%] text-center text-[8px] font-medium leading-tight text-white/90">
            {fallbackCenterLabel}
          </span>
        )}
      </div>

      {/* Orbiting action nodes - evenly spaced between inner circle and dashed ring */}
      {orbitNodes.map((node, index) => {
        const Icon = node.icon;
        const position = orbitPositions[index];
        const angleRad = (position.angle * Math.PI) / 180;

        // Calculate x, y offsets from center (50%, 50%)
        const x = 50 + orbitRadius * Math.sin(angleRad);
        const y = 50 - orbitRadius * Math.cos(angleRad);
        const xPct = `${x.toFixed(4)}%`;
        const yPct = `${y.toFixed(4)}%`;
        const isTopPosition = position.tooltipPosition === "top";

        return (
          <div
            key={index}
            className="group absolute"
            style={{
              left: xPct,
              top: yPct,
              width: `${orbitNodeSize}%`,
              height: `${orbitNodeSize}%`,
              transform: "translate(-50%, -50%)",
            }}
          >
            <button
              onClick={node.onClick}
              className="flex h-full w-full items-center justify-center rounded-full bg-slate-700/80 transition-all duration-300 hover:scale-110 hover:bg-slate-600 hover:shadow-[0_0_12px_rgba(255,255,255,0.3)] focus:outline-none focus:ring-1 focus:ring-white/50"
              aria-label={node.label}
            >
              <Icon className="h-[55%] w-[55%] text-white" />
            </button>
            {/* Tooltip - position above for top node, below for others */}
            <div
              className={`pointer-events-none absolute left-1/2 -translate-x-1/2 whitespace-nowrap rounded bg-slate-900 px-1.5 py-0.5 text-[8px] font-medium text-white opacity-0 shadow-lg transition-opacity duration-200 group-hover:opacity-100 ${
                isTopPosition ? "bottom-full mb-1" : "top-full mt-1"
              }`}
            >
              {node.label}
              <div
                className={`absolute left-1/2 -translate-x-1/2 border-2 border-transparent ${
                  isTopPosition
                    ? "top-full border-t-slate-900"
                    : "-top-0.5 border-b-slate-900"
                }`}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
