//! Locus-level scatter plot renderer for zoomed-in LocusZoom-style views.
//!
//! This module renders a scatter plot for a specific genomic region, distinguishing
//! between Exome and Genome variants with different shapes and colors.

use crate::manhattan::layout::YScale;
use crate::HailError;
use crate::Result;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

/// Data source for a variant (determines shape and color).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSource {
    Genome,
    Exome,
}

/// A variant prepared for rendering in the locus plot.
pub struct RenderVariant {
    pub position: i32,
    pub pvalue: f64,
    pub source: DataSource,
    pub is_significant: bool,
}

/// Configuration for the locus scatter plot.
pub struct LocusPlotConfig {
    pub width: u32,
    pub height: u32,
    /// Genomic start position of the window
    pub start_pos: i32,
    /// Genomic end position of the window
    pub end_pos: i32,
    /// Maximum -log10(p) value for Y-axis scaling
    pub y_max: f64,
}

impl LocusPlotConfig {
    /// Map genomic position to X-pixel coordinate.
    pub fn get_x(&self, pos: i32) -> f32 {
        let range = (self.end_pos - self.start_pos) as f64;
        if range <= 0.0 {
            return 0.0;
        }
        let relative = (pos - self.start_pos) as f64;
        let x_fraction = relative / range;

        // Map to full width [0, width]
        (x_fraction * self.width as f64) as f32
    }

    /// Map p-value to Y-pixel coordinate using hybrid linear-log scale.
    pub fn get_y(&self, pval: f64) -> f32 {
        let neg_log_p = -pval.log10();
        // Use YScale for consistent linear-log behavior
        let scale = YScale::new(self.height, self.y_max);
        scale.get_y(neg_log_p)
    }
}

/// Renderer for locus-level scatter plots.
pub struct LocusRenderer {
    pixmap: Pixmap,
    config: LocusPlotConfig,
}

impl LocusRenderer {
    /// Create a new renderer with a white background.
    pub fn new(config: LocusPlotConfig) -> Self {
        let mut pixmap = Pixmap::new(config.width, config.height)
            .expect("Failed to allocate locus pixmap (dimensions too large?)");
        pixmap.fill(Color::WHITE);
        Self { pixmap, config }
    }

    /// Draw a dashed horizontal line at the significance threshold.
    pub fn draw_threshold_line(&mut self, pvalue: f64) {
        let y = self.config.get_y(pvalue);

        let mut paint = Paint::default();
        paint.set_color_rgba8(183, 28, 28, 128); // Dark Red, semi-transparent
        paint.anti_alias = true;

        let mut stroke = Stroke::default();
        stroke.width = 1.0;
        stroke.dash = tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0);

        let mut pb = PathBuilder::new();
        pb.move_to(0.0, y);
        pb.line_to(self.config.width as f32, y);

        if let Some(path) = pb.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
    }

    /// Render variants as points.
    ///
    /// Draws Genome variants first (background), then Exome variants (foreground).
    /// This layering ensures sparser exome variants aren't buried under genome variants.
    pub fn draw_variants(&mut self, variants: &[RenderVariant]) {
        // Define colors - same for significant and non-significant
        // Genome: Steel Blue #4682B4
        let genome_color = hex_to_color("#4682B4", 0.7);
        // Exome: Orange #F57C00
        let exome_color = hex_to_color("#F57C00", 0.8);

        let radius = 2.0; // Consistent small size for all points

        // Helper to draw variants for a given source
        let draw_source = |pixmap: &mut Pixmap, config: &LocusPlotConfig, source: DataSource, paint_color: Color| {
            let mut paint = Paint::default();
            paint.set_color(paint_color);
            paint.anti_alias = true;

            for v in variants {
                if v.source != source {
                    continue;
                }

                let x = config.get_x(v.position);
                let y = config.get_y(v.pvalue);

                // Skip points far outside bounds
                if x < -5.0 || x > (config.width as f32 + 5.0) {
                    continue;
                }

                match source {
                    DataSource::Genome => {
                        // Circle
                        if let Some(path) = PathBuilder::from_circle(x, y, radius) {
                            pixmap.fill_path(
                                &path,
                                &paint,
                                FillRule::Winding,
                                Transform::identity(),
                                None,
                            );
                        }
                    }
                    DataSource::Exome => {
                        // Diamond (rotated square)
                        let mut pb = PathBuilder::new();
                        pb.move_to(x, y - radius);
                        pb.line_to(x + radius, y);
                        pb.line_to(x, y + radius);
                        pb.line_to(x - radius, y);
                        pb.close();

                        if let Some(path) = pb.finish() {
                            pixmap.fill_path(
                                &path,
                                &paint,
                                FillRule::Winding,
                                Transform::identity(),
                                None,
                            );
                        }
                    }
                }
            }
        };

        // Draw genome first (background), then exome (foreground)
        draw_source(&mut self.pixmap, &self.config, DataSource::Genome, genome_color);
        draw_source(&mut self.pixmap, &self.config, DataSource::Exome, exome_color);
    }

    /// Encode the rendered pixmap as a PNG byte vector.
    pub fn encode_png(&self) -> Result<Vec<u8>> {
        self.pixmap.encode_png().map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("PNG encoding failed: {}", e),
            ))
        })
    }
}

/// Parse a hex color string (e.g. "#4682B4") into a tiny-skia Color with alpha.
fn hex_to_color(hex: &str, alpha: f32) -> Color {
    let hex = hex.trim_start_matches('#');
    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(0);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(0);

    Color::from_rgba8(r, g, b, (alpha * 255.0) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_render_locus_plot() {
        // Setup a mock region: chr1:100,000-200,000
        let config = LocusPlotConfig {
            width: 800,
            height: 400,
            start_pos: 100_000,
            end_pos: 200_000,
            y_max: 20.0,
        };

        let mut renderer = LocusRenderer::new(config);

        // Mock Data
        let variants = vec![
            // Genome background (noise)
            RenderVariant {
                position: 110_000,
                pvalue: 1e-1,
                source: DataSource::Genome,
                is_significant: false,
            },
            RenderVariant {
                position: 120_000,
                pvalue: 1e-2,
                source: DataSource::Genome,
                is_significant: false,
            },
            RenderVariant {
                position: 130_000,
                pvalue: 1e-3,
                source: DataSource::Genome,
                is_significant: false,
            },
            // Genome Significant Hit
            RenderVariant {
                position: 150_000,
                pvalue: 1e-9,
                source: DataSource::Genome,
                is_significant: true,
            },
            // Exome Signal (should appear on top of genome)
            RenderVariant {
                position: 148_000,
                pvalue: 1e-5,
                source: DataSource::Exome,
                is_significant: false,
            },
            RenderVariant {
                position: 150_000,
                pvalue: 1e-12,
                source: DataSource::Exome,
                is_significant: true,
            },
            RenderVariant {
                position: 152_000,
                pvalue: 1e-6,
                source: DataSource::Exome,
                is_significant: false,
            },
        ];

        // Render
        renderer.draw_threshold_line(5e-8);
        renderer.draw_variants(&variants);

        // Save output for visual inspection
        let png_data = renderer.encode_png().unwrap();
        let mut file = File::create("locus_test_render.png").unwrap();
        file.write_all(&png_data).unwrap();

        println!("Test plot saved to locus_test_render.png");
    }
}
