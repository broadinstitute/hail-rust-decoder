import React, { useRef, useState, useEffect, useCallback } from 'react';
import { useHitDetection } from './hooks/useHitDetection';
import { Tooltip } from './components/Tooltip';
import { YAxis } from './components/YAxis';
import type { ManhattanSidecar, SignificantHit } from './types';
import './ManhattanViewer.css';

const Y_AXIS_WIDTH = 50;

export interface ManhattanViewerProps {
  /** URL to the PNG image */
  imageUrl: string;
  /** Parsed sidecar JSON */
  sidecar: ManhattanSidecar;
  /** Callback when a significant hit is clicked */
  onVariantClick?: (hit: SignificantHit) => void;
  /** Callback when hovering over a hit (null = hover out) */
  onVariantHover?: (hit: SignificantHit | null) => void;
  /** Show chromosome labels below the plot */
  showChromLabels?: boolean;
  /** Show Y-axis with -log10(p) labels */
  showYAxis?: boolean;
  /** Show stats bar with hit counts */
  showStats?: boolean;
  /** Minimum width before horizontal scroll kicks in */
  minWidth?: number;
  /** Custom class name for the container */
  className?: string;
}

/**
 * Manhattan Plot Viewer Component.
 *
 * Composites a server-rendered PNG with an interactive SVG overlay
 * for significant variant hits. Uses spatial indexing for efficient
 * hit detection without DOM thrashing.
 */
export const ManhattanViewer: React.FC<ManhattanViewerProps> = ({
  imageUrl,
  sidecar,
  onVariantClick,
  onVariantHover,
  showChromLabels = true,
  showYAxis = true,
  showStats = true,
  minWidth = 800,
  className,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const imageWrapperRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [hoveredHit, setHoveredHit] = useState<SignificantHit | null>(null);
  const [cursor, setCursor] = useState({ x: 0, y: 0 });
  const [imageLoaded, setImageLoaded] = useState(false);
  const [imageError, setImageError] = useState(false);

  // Use spatial indexing for efficient hit detection
  const { findHit } = useHitDetection(
    sidecar.significant_hits,
    dimensions.width,
    dimensions.height
  );

  // Observe image wrapper size changes for responsive scaling
  useEffect(() => {
    const imageWrapper = imageWrapperRef.current;
    if (!imageWrapper) return;

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        const { width } = entry.contentRect;
        // Calculate height maintaining aspect ratio
        const aspectRatio = sidecar.image.height / sidecar.image.width;
        const height = width * aspectRatio;
        setDimensions({ width, height });
      }
    });

    observer.observe(imageWrapper);
    return () => observer.disconnect();
  }, [sidecar.image.width, sidecar.image.height]);

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = e.currentTarget;
      const rect = svg.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      setCursor({ x, y });

      const hit = findHit(x, y);
      if (hit !== hoveredHit) {
        setHoveredHit(hit);
        onVariantHover?.(hit);
      }
    },
    [findHit, hoveredHit, onVariantHover]
  );

  const handleMouseLeave = useCallback(() => {
    setHoveredHit(null);
    onVariantHover?.(null);
  }, [onVariantHover]);

  const handleClick = useCallback(() => {
    if (hoveredHit && onVariantClick) {
      onVariantClick(hoveredHit);
    }
  }, [hoveredHit, onVariantClick]);

  const handleImageLoad = useCallback(() => {
    setImageLoaded(true);
    setImageError(false);
  }, []);

  const handleImageError = useCallback(() => {
    setImageError(true);
    setImageLoaded(false);
  }, []);

  // Calculate highlight position for hovered hit
  const highlightPosition = hoveredHit
    ? {
        x: hoveredHit.x_normalized * dimensions.width,
        y: hoveredHit.y_normalized * dimensions.height,
      }
    : null;

  const containerStyle: React.CSSProperties = {
    minWidth: minWidth,
  };

  if (imageError) {
    return (
      <div className={`manhattan-container ${className || ''}`} style={containerStyle}>
        <div className="manhattan-error">Failed to load Manhattan plot image</div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={`manhattan-container ${className || ''}`}
      style={containerStyle}
    >
      <div className="manhattan-plot-row" style={{ display: 'flex' }}>
        {/* Y-Axis */}
        {showYAxis && imageLoaded && sidecar.y_axis && (
          <div style={{ width: Y_AXIS_WIDTH, flexShrink: 0, position: 'relative' }}>
            <YAxis
              scale={sidecar.y_axis}
              height={dimensions.height}
              width={Y_AXIS_WIDTH}
            />
          </div>
        )}

        <div ref={imageWrapperRef} className="manhattan-image-wrapper" style={{ flex: 1 }}>
          <img
            src={imageUrl}
            alt="Manhattan Plot"
            className="manhattan-image"
            onLoad={handleImageLoad}
            onError={handleImageError}
            draggable={false}
          />

          {imageLoaded && dimensions.width > 0 && (
            <svg
              className="manhattan-overlay"
              viewBox={`0 0 ${dimensions.width} ${dimensions.height}`}
              preserveAspectRatio="none"
              onMouseMove={handleMouseMove}
              onMouseLeave={handleMouseLeave}
              onClick={handleClick}
            >
              {/* Highlight circle for hovered hit */}
              {highlightPosition && (
                <g>
                  <circle
                    cx={highlightPosition.x}
                    cy={highlightPosition.y}
                    r={8}
                    className="manhattan-highlight-inner"
                  />
                  <circle
                    cx={highlightPosition.x}
                    cy={highlightPosition.y}
                    r={8}
                    className="manhattan-highlight"
                  />
                </g>
              )}
            </svg>
          )}

          {/* Tooltip */}
          {hoveredHit && imageLoaded && (
            <Tooltip
              hit={hoveredHit}
              x={cursor.x}
              y={cursor.y}
              containerWidth={dimensions.width}
            />
          )}
        </div>
      </div>

      {/* Chromosome labels */}
      {showChromLabels && imageLoaded && (
        <div
          className="manhattan-chrom-labels"
          style={{ marginLeft: showYAxis ? Y_AXIS_WIDTH : 0 }}
        >
          {sidecar.chromosomes.map((chrom) => {
            const centerPercent =
              ((chrom.x_start_px + chrom.x_end_px) / 2 / sidecar.image.width) * 100;
            return (
              <span
                key={chrom.name}
                className="manhattan-chrom-label"
                style={{
                  left: `${centerPercent}%`,
                  color: chrom.color,
                }}
              >
                {chrom.name}
              </span>
            );
          })}
        </div>
      )}

      {/* Stats bar */}
      {showStats && imageLoaded && (
        <div className="manhattan-stats">
          <div className="manhattan-stats-item">
            <span className="manhattan-stats-label">Significant hits:</span>
            <span className="manhattan-stats-value">
              {sidecar.significant_hits.length.toLocaleString()}
            </span>
          </div>
          <div className="manhattan-stats-item">
            <span className="manhattan-stats-label">Threshold:</span>
            <span className="manhattan-stats-value">
              P &lt; {sidecar.threshold.pvalue.toExponential(0)}
            </span>
          </div>
          <div className="manhattan-stats-item">
            <span className="manhattan-stats-label">Chromosomes:</span>
            <span className="manhattan-stats-value">{sidecar.chromosomes.length}</span>
          </div>
        </div>
      )}

      {/* Loading state */}
      {!imageLoaded && !imageError && (
        <div className="manhattan-loading">Loading Manhattan plot...</div>
      )}
    </div>
  );
};
