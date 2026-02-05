//! Rendering engine using tiny-skia for Manhattan plot rasterization.

use crate::HailError;
use crate::Result;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

/// Rasterizes points onto a pixel buffer and encodes to PNG.
pub struct ManhattanRenderer {
    pixmap: Pixmap,
}

impl ManhattanRenderer {
    /// Create a new renderer with a white background.
    pub fn new(width: u32, height: u32) -> Self {
        let mut pixmap =
            Pixmap::new(width, height).expect("Failed to allocate pixmap (dimensions too large?)");
        pixmap.fill(Color::WHITE);
        Self { pixmap }
    }

    /// Create a new renderer with a transparent background.
    /// Use this for distributed rendering where partial images will be composited.
    pub fn new_transparent(width: u32, height: u32) -> Self {
        let pixmap =
            Pixmap::new(width, height).expect("Failed to allocate pixmap (dimensions too large?)");
        // Pixmap is already transparent (zeroed) by default
        Self { pixmap }
    }

    /// Render a single variant point as an anti-aliased circle.
    pub fn render_point(&mut self, x: f32, y: f32, color_hex: &str, alpha: f32) {
        let mut paint = Paint::default();
        paint.set_color(hex_to_color(color_hex, alpha));
        paint.anti_alias = true;

        if let Some(path) = PathBuilder::from_circle(x, y, 2.5) {
            self.pixmap
                .fill_path(&path, &paint, FillRule::Winding, Transform::identity(), None);
        }
    }

    /// Draw a horizontal dashed threshold line.
    pub fn render_threshold_line(&mut self, y: f32, width: u32) {
        let mut paint = Paint::default();
        paint.set_color(hex_to_color("#B71C1C", 0.5));
        paint.anti_alias = true;

        let mut stroke = Stroke::default();
        stroke.width = 1.0;
        stroke.dash = tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0);

        let mut pb = PathBuilder::new();
        pb.move_to(0.0, y);
        pb.line_to(width as f32, y);
        if let Some(path) = pb.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
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
