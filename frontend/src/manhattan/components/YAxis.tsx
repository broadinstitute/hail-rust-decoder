import React from 'react';
import type { YAxisScale } from '../types';

interface YAxisProps {
  /** Y-axis scale metadata from sidecar */
  scale: YAxisScale;
  /** Current rendered height of the plot */
  height: number;
  /** Width of the Y-axis area (for positioning) */
  width?: number;
}

/**
 * Generate tick values for the Y-axis.
 * Uses nice round numbers and ensures coverage of both linear and log regions.
 */
function generateTicks(scale: YAxisScale): number[] {
  const ticks: number[] = [];

  // Linear region: 0 to log_threshold (typically 0-10)
  // Use integer steps
  for (let i = 0; i <= scale.log_threshold; i += 2) {
    ticks.push(i);
  }

  // Log region: above log_threshold
  // Use powers of 10 and intermediate values
  const logStart = Math.ceil(Math.log10(scale.log_threshold));
  const logEnd = Math.floor(Math.log10(scale.max_neg_log_p));

  for (let exp = logStart; exp <= logEnd; exp++) {
    const val = Math.pow(10, exp);
    if (val > scale.log_threshold && val <= scale.max_neg_log_p) {
      ticks.push(val);
    }
    // Add 3x intermediate for better coverage
    const intermediate = 3 * Math.pow(10, exp);
    if (intermediate > scale.log_threshold && intermediate <= scale.max_neg_log_p) {
      ticks.push(intermediate);
    }
  }

  return ticks.sort((a, b) => a - b);
}

/**
 * Convert a -log10(p) value to pixel Y position using the hybrid linear-log scale.
 * This mirrors the Rust YScale::get_y function.
 */
function valueToY(val: number, scale: YAxisScale, height: number): number {
  const linearHeight = height * scale.linear_fraction;
  const logHeight = height * (1 - scale.linear_fraction);

  if (val <= scale.log_threshold) {
    // Linear portion: maps [0, log_threshold] -> [height, height - linearHeight]
    const normalized = val / scale.log_threshold;
    return height - normalized * linearHeight;
  } else {
    // Log portion: maps [log_threshold, max_neg_log_p] -> [height - linearHeight, 0]
    const logVal = Math.log(val / scale.log_threshold);
    const logMax = Math.log(scale.max_neg_log_p / scale.log_threshold);
    const normalized = Math.min(logVal / logMax, 1.0);

    // Position in the log portion
    const yInLog = normalized * logHeight;
    return Math.max(height - linearHeight - yInLog, 0);
  }
}

/**
 * Format a tick value for display.
 */
function formatTick(val: number): string {
  if (val === 0) return '0';
  if (val <= 100) return val.toString();
  // Use scientific notation for large values
  const exp = Math.floor(Math.log10(val));
  const mantissa = val / Math.pow(10, exp);
  if (mantissa === 1) {
    return `10${superscript(exp)}`;
  }
  return `${mantissa}×10${superscript(exp)}`;
}

/**
 * Convert a number to superscript characters.
 */
function superscript(n: number): string {
  const superscripts: Record<string, string> = {
    '0': '\u2070',
    '1': '\u00B9',
    '2': '\u00B2',
    '3': '\u00B3',
    '4': '\u2074',
    '5': '\u2075',
    '6': '\u2076',
    '7': '\u2077',
    '8': '\u2078',
    '9': '\u2079',
  };
  return n
    .toString()
    .split('')
    .map((d) => superscripts[d] || d)
    .join('');
}

/**
 * Y-Axis component for the Manhattan plot.
 * Renders tick marks and labels for the hybrid linear-log scale.
 */
export const YAxis: React.FC<YAxisProps> = ({ scale, height, width = 50 }) => {
  const ticks = generateTicks(scale);

  return (
    <div
      className="manhattan-yaxis"
      style={{
        position: 'absolute',
        left: 0,
        top: 0,
        width: width,
        height: height,
        display: 'flex',
        flexDirection: 'column',
        pointerEvents: 'none',
      }}
    >
      {/* Axis label */}
      <div
        style={{
          position: 'absolute',
          left: 0,
          top: height / 2,
          transform: 'rotate(-90deg) translateX(-50%)',
          transformOrigin: '0 0',
          fontSize: '11px',
          color: '#666',
          whiteSpace: 'nowrap',
        }}
      >
        -log₁₀(p)
      </div>

      {/* Tick marks and labels */}
      {ticks.map((val) => {
        const y = valueToY(val, scale, height);
        // Skip ticks that would be too close to edges
        if (y < 5 || y > height - 5) return null;

        return (
          <div
            key={val}
            style={{
              position: 'absolute',
              right: 0,
              top: y,
              transform: 'translateY(-50%)',
              display: 'flex',
              alignItems: 'center',
              fontSize: '10px',
              color: '#666',
            }}
          >
            <span style={{ marginRight: '4px' }}>{formatTick(val)}</span>
            <div
              style={{
                width: '4px',
                height: '1px',
                backgroundColor: '#ccc',
              }}
            />
          </div>
        );
      })}
    </div>
  );
};
