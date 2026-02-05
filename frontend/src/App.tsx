import React, { useState, useEffect } from 'react';
import { ManhattanViewer } from './manhattan';
import type { ManhattanSidecar, SignificantHit } from './manhattan';

// Default paths - can be customized via URL params or environment
const DEFAULT_IMAGE_URL = '/manhattan.png';
const DEFAULT_SIDECAR_URL = '/manhattan.json';

function App() {
  const [sidecar, setSidecar] = useState<ManhattanSidecar | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedHit, setSelectedHit] = useState<SignificantHit | null>(null);

  // Parse URL params for custom paths
  const params = new URLSearchParams(window.location.search);
  const imageUrl = params.get('image') || DEFAULT_IMAGE_URL;
  const sidecarUrl = params.get('sidecar') || DEFAULT_SIDECAR_URL;

  useEffect(() => {
    async function loadSidecar() {
      try {
        const response = await fetch(sidecarUrl);
        if (!response.ok) {
          throw new Error(`Failed to load sidecar: ${response.status}`);
        }
        const data = await response.json();
        setSidecar(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      }
    }

    loadSidecar();
  }, [sidecarUrl]);

  const handleVariantClick = (hit: SignificantHit) => {
    setSelectedHit(hit);
    console.log('Variant clicked:', hit);
  };

  if (error) {
    return (
      <div style={styles.container}>
        <div style={styles.error}>
          <h2>Error Loading Manhattan Plot</h2>
          <p>{error}</p>
          <p style={styles.hint}>
            Make sure manhattan.png and manhattan.json are available at the specified paths.
          </p>
        </div>
      </div>
    );
  }

  if (!sidecar) {
    return (
      <div style={styles.container}>
        <div style={styles.loading}>Loading Manhattan plot data...</div>
      </div>
    );
  }

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Manhattan Plot Viewer</h1>
        <p style={styles.subtitle}>
          Interactive visualization of GWAS results
        </p>
      </header>

      <main style={styles.main}>
        <ManhattanViewer
          imageUrl={imageUrl}
          sidecar={sidecar}
          onVariantClick={handleVariantClick}
          showChromLabels={true}
          showStats={true}
        />

        {/* Selected variant detail panel */}
        {selectedHit && (
          <div style={styles.detailPanel}>
            <div style={styles.detailHeader}>
              <h3 style={styles.detailTitle}>Selected Variant</h3>
              <button
                onClick={() => setSelectedHit(null)}
                style={styles.closeButton}
              >
                ×
              </button>
            </div>
            <div style={styles.detailContent}>
              <div style={styles.detailRow}>
                <span style={styles.detailLabel}>Variant ID:</span>
                <span style={styles.detailValue}>{selectedHit.variant_id}</span>
              </div>
              <div style={styles.detailRow}>
                <span style={styles.detailLabel}>P-value:</span>
                <span style={styles.detailValue}>
                  {selectedHit.pvalue.toExponential(4)}
                </span>
              </div>
              <div style={styles.detailRow}>
                <span style={styles.detailLabel}>Position (px):</span>
                <span style={styles.detailValue}>
                  ({selectedHit.x_px.toFixed(1)}, {selectedHit.y_px.toFixed(1)})
                </span>
              </div>
              {selectedHit.annotations &&
                Object.entries(selectedHit.annotations).map(([key, value]) => (
                  <div key={key} style={styles.detailRow}>
                    <span style={styles.detailLabel}>{key}:</span>
                    <span style={styles.detailValue}>{String(value)}</span>
                  </div>
                ))}
            </div>
          </div>
        )}
      </main>

      <footer style={styles.footer}>
        <p>
          Powered by{' '}
          <a
            href="https://github.com/your-org/hail-decoder"
            style={styles.link}
          >
            hail-decoder
          </a>
        </p>
      </footer>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    maxWidth: '1400px',
    margin: '0 auto',
    padding: '20px',
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column',
  },
  header: {
    marginBottom: '24px',
  },
  title: {
    fontSize: '28px',
    fontWeight: 600,
    color: '#1a1a1a',
    margin: 0,
  },
  subtitle: {
    fontSize: '14px',
    color: '#666',
    margin: '4px 0 0',
  },
  main: {
    flex: 1,
  },
  footer: {
    marginTop: '32px',
    paddingTop: '16px',
    borderTop: '1px solid #eee',
    fontSize: '12px',
    color: '#999',
    textAlign: 'center',
  },
  link: {
    color: '#4682B4',
    textDecoration: 'none',
  },
  loading: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '400px',
    color: '#666',
  },
  error: {
    textAlign: 'center',
    padding: '40px',
    color: '#dc3545',
  },
  hint: {
    fontSize: '14px',
    color: '#666',
  },
  detailPanel: {
    marginTop: '24px',
    padding: '16px',
    backgroundColor: '#f8f9fa',
    borderRadius: '8px',
    border: '1px solid #e9ecef',
  },
  detailHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '12px',
  },
  detailTitle: {
    margin: 0,
    fontSize: '16px',
    fontWeight: 600,
    color: '#333',
  },
  closeButton: {
    background: 'none',
    border: 'none',
    fontSize: '24px',
    cursor: 'pointer',
    color: '#666',
    padding: '0 8px',
  },
  detailContent: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
    gap: '8px',
  },
  detailRow: {
    display: 'flex',
    gap: '8px',
    fontSize: '13px',
  },
  detailLabel: {
    color: '#666',
    fontWeight: 500,
  },
  detailValue: {
    color: '#333',
    fontFamily: 'monospace',
  },
};

export default App;
