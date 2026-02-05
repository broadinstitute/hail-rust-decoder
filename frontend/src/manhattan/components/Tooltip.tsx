import React from 'react';
import type { SignificantHit } from '../types';

interface TooltipProps {
  hit: SignificantHit;
  x: number;
  y: number;
  containerWidth: number;
}

/**
 * Tooltip component for displaying variant information on hover.
 * Automatically positions itself to avoid overflowing the viewport edge.
 */
export const Tooltip: React.FC<TooltipProps> = ({ hit, x, y, containerWidth }) => {
  // Flip tooltip to the left if cursor is near right edge
  const flipThreshold = 0.8;
  const shouldFlip = x > containerWidth * flipThreshold;

  const style: React.CSSProperties = {
    position: 'absolute',
    top: y - 10,
    left: shouldFlip ? undefined : x + 15,
    right: shouldFlip ? containerWidth - x + 15 : undefined,
    backgroundColor: 'rgba(0, 0, 0, 0.85)',
    color: '#fff',
    padding: '8px 12px',
    borderRadius: '4px',
    fontSize: '13px',
    lineHeight: '1.4',
    pointerEvents: 'none',
    zIndex: 1000,
    maxWidth: '300px',
    whiteSpace: 'nowrap',
    boxShadow: '0 2px 8px rgba(0, 0, 0, 0.3)',
  };

  const formatPvalue = (p: number): string => {
    if (p < 1e-100) {
      return '< 1e-100';
    }
    return p.toExponential(2);
  };

  return (
    <div style={style} className="manhattan-tooltip">
      <div style={{ fontWeight: 'bold', marginBottom: '4px' }}>{hit.variant_id}</div>
      <div style={{ color: '#aaa' }}>
        P = <span style={{ color: '#ff6b6b' }}>{formatPvalue(hit.pvalue)}</span>
      </div>
      {hit.annotations && Object.keys(hit.annotations).length > 0 && (
        <div style={{ marginTop: '6px', borderTop: '1px solid #444', paddingTop: '6px' }}>
          {Object.entries(hit.annotations).map(([key, value]) => (
            <div key={key} style={{ color: '#ccc', fontSize: '12px' }}>
              <span style={{ color: '#888' }}>{key}:</span> {String(value)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};
