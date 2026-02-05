import { useMemo, useCallback } from 'react';
import type { SignificantHit } from '../types';

const DEFAULT_CELL_SIZE = 20; // pixels
const DEFAULT_HIT_RADIUS = 10; // pixels - how close cursor must be to register a hit

interface HitDetectionOptions {
  cellSize?: number;
  hitRadius?: number;
}

interface HitDetectionResult {
  findHit: (x: number, y: number) => SignificantHit | null;
}

/**
 * Spatial indexing hook for efficient hit detection on Manhattan plot overlays.
 *
 * Instead of checking every hit on every mouse move, this hook builds a grid-based
 * spatial index that allows O(1) lookup of nearby candidates.
 *
 * @param hits - Array of significant hits from the sidecar JSON
 * @param width - Current rendered width of the plot
 * @param height - Current rendered height of the plot
 * @param options - Optional configuration for cell size and hit radius
 */
export function useHitDetection(
  hits: SignificantHit[],
  width: number,
  height: number,
  options: HitDetectionOptions = {}
): HitDetectionResult {
  const { cellSize = DEFAULT_CELL_SIZE, hitRadius = DEFAULT_HIT_RADIUS } = options;

  // Build spatial grid index when hits or dimensions change
  const hitGrid = useMemo(() => {
    const grid = new Map<string, SignificantHit[]>();

    if (width <= 0 || height <= 0) {
      return grid;
    }

    for (const hit of hits) {
      // Convert normalized coordinates to current pixel coordinates
      const pixelX = hit.x_normalized * width;
      const pixelY = hit.y_normalized * height;

      // Calculate grid cell coordinates
      const gridX = Math.floor(pixelX / cellSize);
      const gridY = Math.floor(pixelY / cellSize);
      const key = `${gridX},${gridY}`;

      const existing = grid.get(key);
      if (existing) {
        existing.push(hit);
      } else {
        grid.set(key, [hit]);
      }
    }

    return grid;
  }, [hits, width, height, cellSize]);

  // Find hit at cursor position by checking nearby grid cells
  const findHit = useCallback(
    (mouseX: number, mouseY: number): SignificantHit | null => {
      if (width <= 0 || height <= 0) {
        return null;
      }

      const gridX = Math.floor(mouseX / cellSize);
      const gridY = Math.floor(mouseY / cellSize);

      // Check the current cell and all 8 neighboring cells
      const candidates: SignificantHit[] = [];
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const key = `${gridX + dx},${gridY + dy}`;
          const cellHits = hitGrid.get(key);
          if (cellHits) {
            candidates.push(...cellHits);
          }
        }
      }

      if (candidates.length === 0) {
        return null;
      }

      // Find the closest hit within the hit radius
      let closestHit: SignificantHit | null = null;
      let closestDistance = hitRadius;

      for (const hit of candidates) {
        const hitPixelX = hit.x_normalized * width;
        const hitPixelY = hit.y_normalized * height;

        const distance = Math.sqrt(
          Math.pow(mouseX - hitPixelX, 2) + Math.pow(mouseY - hitPixelY, 2)
        );

        if (distance < closestDistance) {
          closestDistance = distance;
          closestHit = hit;
        }
      }

      return closestHit;
    },
    [hitGrid, width, height, cellSize, hitRadius]
  );

  return { findHit };
}
