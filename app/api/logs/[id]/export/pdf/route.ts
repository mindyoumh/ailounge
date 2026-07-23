import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { PDFDocument, PDFPage, PDFFont, rgb, StandardFonts } from "pdf-lib";

export const dynamic = "force-dynamic";

function sanitize(s: string): string {
  return s.replace(/[^\x20-\x7E\u00A0-\u00FF]/g, "").trim();
}

function truncate(text: string, maxLen: number): string {
  if (text.length <= maxLen) return text;
  return text.substring(0, maxLen) + "...";
}

function patternSummary(key: string): string {
  const clean = sanitize(key);
  if (clean.length <= 120) return clean;
  return truncate(clean, 80) + " — See dashboard drill-down for complete details.";
}

function anomalySummary(desc: string): string {
  const clean = sanitize(desc);
  const stripped = clean.replace(/^Error spike on \d{4}-\d{2}-\d{2}:?\s*/, "");
  if (stripped.length > 100) return truncate(stripped, 100);
  return stripped || clean;
}

function findingTitle(key: string): string {
  const raw = sanitize(key);
  if (!raw) return "Unknown Error";
  let t = raw.replace(/^\d{3}:\s*/i, "");
  const taskMatch = t.match(/^[Tt]he task\s+(\S+)\s+has failed/i);
  if (taskMatch) {
    const task = taskMatch[1].toLowerCase();
    if (task.includes("sync_session")) return "Session Sync Failure";
    if (task.includes("sync_user")) return "User Sync Failure";
    if (task.includes("sync")) return "Sync Failure";
    return "Task Execution Failure";
  }
  const codeMatch = t.match(/^[A-Z_]+:\s*(.*)/);
  if (codeMatch) t = codeMatch[1];
  const lower = t.toLowerCase();
  if (lower.includes("time") && (lower.includes("available") || lower.includes("slot"))) return "Scheduling Conflict";
  if (lower.includes("doctor") && lower.includes("appointment")) return "Doctor Calendar Mismatch";
  if (lower.includes("far enough") || lower.includes("in advance")) return "Advance Booking Window";
  if (lower.includes("validation")) return "Validation Error";
  if (lower.includes("not authorized") || lower.includes("forbidden")) return "Unauthorized Access";
  if (lower.includes("timeout")) return "Connection Timeout";
  if (lower.includes("not found")) return "Resource Not Found";
  if (lower.includes("invalid data")) return "Invalid Data";
  if (lower.includes("duplicate")) return "Duplicate Data";
  if (lower.includes("limit exceeded") || (lower.includes("limit") && lower.includes("exceed"))) return "Record Limit Exceeded";
  if (lower.includes("rate limit")) return "Rate Limit Exceeded";
  if (lower.includes("batch sync") && lower.includes("missing")) return "Missing Data In Batch Sync";
  if (lower.includes("batch sync")) return "Batch Sync Error";
  t = t.replace(/\{var\}/g, "").replace(/\s+/g, " ").trim();
  t = t.replace(/\b(id|record|user|appointment)\s+\d+\b/gi, "").trim();
  t = t.replace(/[\w.+-]+@[\w-]+\.[\w.-]+/g, "").trim();
  t = t.replace(/\+?\d[\d\s\-().]{7,}\d/g, "").trim();
  if (t.length > 55) t = truncate(t, 55);
  if (!t) return "Unknown Error";
  return t.charAt(0).toUpperCase() + t.slice(1).toLowerCase();
}

function frequencySeverity(count: number): { label: string; color: ReturnType<typeof rgb> } {
  if (count > 1000) return { label: "HIGH", color: COL.red };
  if (count > 100) return { label: "MEDIUM", color: COL.amber };
  return { label: "LOW", color: COL.green };
}

// ─── color palette (module-level, accessible everywhere) ───────────────

function rgbC(r: number, g: number, b: number) {
  return rgb(r, g, b);
}

const COL = {
  primary:   rgbC(0.15, 0.15, 0.15),
  secondary: rgbC(0.4,  0.4,  0.4),
  muted:     rgbC(0.6,  0.6,  0.6),
  red:       rgbC(0.8,  0.2,  0.2),
  amber:     rgbC(0.8,  0.55, 0.1),
  green:     rgbC(0.15, 0.6,  0.35),
  accent:    rgbC(0.25, 0.4,  0.85),
  border:    rgbC(0.85, 0.85, 0.85),
  white:     rgbC(1,    1,    1),
  headerBg:  rgbC(0.95, 0.95, 0.95),
};

// ─── PdfReport: layout engine ──────────────────────────────────────────

class PdfReport {
  private readonly MARGIN = 50;
  private readonly PAGE_W = 612;
  private readonly PAGE_H = 792;
  private readonly CONTENT_W = 512; // PAGE_W - 2 * MARGIN
  private readonly FOOTER_Y = 40;

  private doc!: PDFDocument;
  private page!: PDFPage;
  private y!: number;
  private font!: PDFFont;
  private boldFont!: PDFFont;
  private pageNum = 0;

  async init() {
    this.doc = await PDFDocument.create();
    this.font = await this.doc.embedFont(StandardFonts.Helvetica);
    this.boldFont = await this.doc.embedFont(StandardFonts.HelveticaBold);
    this.addPage();
  }

  getFonts() {
    return { normal: this.font, bold: this.boldFont };
  }

  /* ── page management ───────────────────────────────────────────── */

  private addPage() {
    this.page = this.doc.addPage([this.PAGE_W, this.PAGE_H]);
    this.y = this.PAGE_H - this.MARGIN;
    this.pageNum++;
  }

  /** Ensure at least `needed` pts of space remain on this page.
   *  Returns true if a page break occurred. */
  ensureSpace(needed: number): boolean {
    if (this.y - needed < this.FOOTER_Y + 10) {
      this.addPage();
      return true;
    }
    return false;
  }

  /* ── low-level draws ───────────────────────────────────────────── */

  private drawText(text: string, x: number, baseline: number, size: number, color: ReturnType<typeof rgb>, f: PDFFont) {
    this.page.drawText(text, { x, y: baseline, size, font: f, color });
  }

  private drawLine(yPos: number, color?: ReturnType<typeof rgb>, t?: number) {
    this.page.drawLine({
      start: { x: this.MARGIN, y: yPos },
      end:   { x: this.PAGE_W - this.MARGIN, y: yPos },
      thickness: t ?? 0.5,
      color: color ?? COL.border,
    });
  }

  private rect(x: number, yBottom: number, w: number, h: number, fill?: ReturnType<typeof rgb>) {
    this.page.drawRectangle({
      x, y: yBottom, width: w, height: h,
      borderColor: COL.border, borderWidth: 1,
      color: fill ?? COL.white,
    });
  }

  /* ── content sections ──────────────────────────────────────────── */

  /** Draw the report header (title + metadata). Only called once on page 1. */
  drawHeader(filename: string, source: string, rangeStart: string, rangeEnd: string) {
    this.drawText("Log Analysis Report", this.MARGIN, this.y - 22, 22, COL.primary, this.boldFont);
    this.y -= 28;

    const meta = [
      `"${sanitize(filename)}"`,
      `Source: ${source}`,
    ];
    if (rangeStart) {
      meta.push(`Date range: ${rangeStart.substring(0, 10)} — ${rangeEnd.substring(0, 10)}`);
    }
    meta.push(`Generated: ${new Date().toISOString().replace("T", " ").substring(0, 19)}`);

    for (const line of meta) {
      this.drawText(line, this.MARGIN, this.y - 10, 10, COL.secondary, this.font);
      this.y -= 14;
    }
    this.y -= 6;
  }

  /** Draw a section title with underline.
   *  Ensures room for the title plus minRemaining pt of content below. */
  sectionTitle(text: string, minRemaining = 50) {
    const overhead = 32; // 4 (margin-before) + 18 (title-text) + 10 (separator)
    if (this.y - overhead - minRemaining < this.FOOTER_Y + 10) {
      this.addPage();
    }
    this.y -= 4;
    this.drawText(text, this.MARGIN, this.y - 14, 14, COL.primary, this.boldFont);
    this.y -= 18;
    this.drawLine(this.y, COL.border, 1);
    this.y -= 10;
  }

  /** Draw a word-wrapped paragraph. */
  drawParagraph(text: string, size = 10, color: ReturnType<typeof rgb> = COL.secondary) {
    const lines = this.wrapText(text, size);
    for (const line of lines) {
      this.ensureSpace(size + 4);
      this.drawText(line, this.MARGIN, this.y - size, size, color, this.font);
      this.y -= size + 4;
    }
  }

  /** Draw a centred empty-state message. */
  drawEmpty(text: string) {
    this.drawText(text, this.MARGIN, this.y - 10, 10, COL.muted, this.font);
    this.y -= 14;
  }

  /** Draw a single line of text at current y position. */
  textLine(text: string, size = 10, color: ReturnType<typeof rgb> = COL.secondary, f?: PDFFont) {
    this.ensureSpace(size + 4);
    this.drawText(text, this.MARGIN, this.y - size, size, color, f ?? this.font);
    this.y -= size + 4;
    return this;
  }

  /** Draw "(continued)" header for pages 2+. */
  drawRunningHeader() {
    this.ensureSpace(16);
    this.drawText("Log Analysis Report (continued)", this.MARGIN, this.y - 10, 10, COL.muted, this.font);
    this.y -= 16;
  }

  /** Draw bullet-point lines. Word-wraps and prefixes each line with "• ". */
  drawBulletLines(lines: string[], size = 9, color: ReturnType<typeof rgb> = COL.secondary) {
    for (const line of lines) {
      const wrapped = this.wrapText(line, size, this.CONTENT_W - 10);
      for (let li = 0; li < wrapped.length; li++) {
        const prefix = li === 0 ? "• " : "  ";
        this.ensureSpace(size + 4);
        this.drawText(prefix + wrapped[li], this.MARGIN, this.y - size, size, color, this.font);
        this.y -= size + 4;
      }
    }
    this.y -= 4;
  }

  /** Draw "— End of Report —" centred near the page bottom. */
  drawEndMarker() {
    this.ensureSpace(24);
    this.y -= 8;
    const text = "— End of Report —";
    const w = this.font.widthOfTextAtSize(text, 10);
    const x = (this.PAGE_W - w) / 2;
    this.drawText(text, x, this.y - 10, 10, COL.muted, this.font);
    this.y -= 20;
  }

  /** Manually adjust vertical position (positive value moves content down). */
  addSpace(pts: number) {
    this.y -= pts;
  }

  /* ── metrics grid ──────────────────────────────────────────────── */

  drawMetricGrid(metrics: [string, string][]) {
    this.ensureSpace(80);
    const boxW = (this.CONTENT_W - 20) / 3;
    const rowH = 36;

    for (let row = 0; row < 2; row++) {
      for (let col = 0; col < 3; col++) {
        const idx = row * 3 + col;
        if (idx >= metrics.length) break;
        const [label, value] = metrics[idx];
        const x = this.MARGIN + col * (boxW + 10);
        const top = this.y - 4 - row * rowH;
        this.rect(x, top - 32, boxW, 32);
        this.drawText(value, x + 6, top - 16, 13, COL.primary, this.boldFont);
        this.drawText(label,  x + 6, top - 26, 8, COL.muted,   this.font);
      }
    }
    this.y -= 2 * rowH + 8;
  }

  /* ── table sections ────────────────────────────────────────────── */

  /** Draw a header row for any table. Call BEFORE drawing data rows. */
  drawTableHeader(
    columns: { x: number; label: string; size?: number; align?: "left" | "right" }[],
    colWidths: number[],
  ) {
    this.y -= 1;
    this.rect(this.MARGIN, this.y - 16, this.CONTENT_W, 16, COL.headerBg);
    for (let ci = 0; ci < columns.length; ci++) {
      const col = columns[ci];
      const cw = colWidths[ci] ?? 100;
      const cx = col.align === "right"
        ? col.x + cw - 4 - this.boldFont.widthOfTextAtSize(col.label, col.size ?? 8)
        : col.x + 4;
      this.drawText(col.label, cx, this.y - 11, col.size ?? 8, COL.secondary, this.boldFont);
    }
    this.y -= 18;
  }

  /** Draw a single data row with wrapped text and dynamic height. */
  drawTableRow(
    cells: { x: number; text: string; size?: number; color?: ReturnType<typeof rgb>; font?: PDFFont; align?: "left" | "right" }[],
    colWidths: number[],
    rowHeight: number,
  ) {
    const lineHeight = 13;
    for (let ci = 0; ci < cells.length; ci++) {
      const c = cells[ci];
      const cw = (colWidths[ci] ?? 100) - 8;
      const lines = this.wrapText(c.text, c.size ?? 8, Math.max(cw, 10));
      const cellFont = c.font ?? this.font;
      for (let li = 0; li < lines.length; li++) {
        const cx = c.align === "right"
          ? c.x + colWidths[ci] - 4 - cellFont.widthOfTextAtSize(lines[li], c.size ?? 8)
          : c.x + 4;
        this.drawText(lines[li], cx, this.y - (c.size ?? 8) - li * lineHeight, c.size ?? 8, c.color ?? COL.primary, cellFont);
      }
    }
    this.y -= rowHeight;
    this.drawLine(this.y + 2, COL.border, 0.3);
    const rightX = this.MARGIN + this.CONTENT_W;
    this.page.drawLine({
      start: { x: rightX, y: this.y + 2 },
      end: { x: rightX, y: this.y + 2 + rowHeight },
      thickness: 0.3,
      color: COL.border,
    });
  }

  /**
   * Draw a table with auto page-break, header repetition, and
   * dynamic row heights. Returns the number of rows drawn.
   */
  drawTable<T>(
    columns: { x: number; label: string; size?: number; align?: "left" | "right" }[],
    rows: T[],
    getCells: (row: T, i: number) => { x: number; text: string; color?: ReturnType<typeof rgb>; font?: PDFFont; size?: number; align?: "left" | "right" }[],
    _minRowHeight = 16,
    maxRows?: number,
  ): number {
    if (rows.length === 0) return 0;

    const colWidths = this.computeColumnWidths(columns);
    const totalW = colWidths.reduce((s, w) => s + w, 0);
    if (totalW > this.CONTENT_W) {
      throw new Error(`PDF table width ${totalW}pt exceeds page width ${this.CONTENT_W}pt`);
    }

    const limit = maxRows ?? rows.length;
    let drawn = 0;
    let needHeader = true;

    for (let i = 0; i < limit; i++) {
      const cells = getCells(rows[i], i);
      const rh = Math.max(this.computeRowHeight(cells, colWidths), _minRowHeight);
      const spaceNeeded = needHeader ? rh + 20 : rh + 2;
      const broke = this.ensureSpace(spaceNeeded);
      if (broke || needHeader) {
        this.drawTableHeader(columns, colWidths);
        needHeader = false;
      }
      this.drawTableRow(cells, colWidths, rh);
      drawn++;
    }

    return drawn;
  }

  /** Compute column widths from adjacent x positions. */
  private computeColumnWidths(
    columns: { x: number; label: string; size?: number; align?: "left" | "right" }[],
  ): number[] {
    return columns.map((col, i) => {
      const nextX = i < columns.length - 1
        ? columns[i + 1].x
        : this.PAGE_W - this.MARGIN;
      return nextX - col.x;
    });
  }

  /** Compute the required height for a row by wrapping each cell. */
  private computeRowHeight(
    cells: { x: number; text: string; size?: number; color?: ReturnType<typeof rgb>; font?: PDFFont; align?: "left" | "right" }[],
    colWidths: number[],
  ): number {
    const lineHeight = 13;
    let maxLines = 1;
    for (let ci = 0; ci < cells.length; ci++) {
      const c = cells[ci];
      const w = (colWidths[ci] ?? 100) - 8;
      const lines = this.wrapText(c.text, c.size ?? 8, Math.max(w, 10));
      maxLines = Math.max(maxLines, lines.length);
    }
    return Math.max(maxLines * lineHeight + 2, 16);
  }

  /* ── utilities ─────────────────────────────────────────────────── */

  private wrapText(text: string, size: number, maxWidth?: number): string[] {
    const mw = maxWidth ?? this.CONTENT_W;
    const words = text.split(/\s+/);
    const lines: string[] = [];
    let line = "";
    for (const word of words) {
      const test = line ? line + " " + word : word;
      if (this.font.widthOfTextAtSize(test, size) > mw) {
        if (line) { lines.push(line); line = word; }
        else { lines.push(test); line = ""; }
      } else {
        line = test;
      }
    }
    if (line) lines.push(line);
    return lines.length ? lines : [""];
  }

  /* ── finalise ──────────────────────────────────────────────────── */

  async finish(filename: string): Promise<NextResponse> {
    const total = this.doc.getPageCount();
    for (let i = 0; i < total; i++) {
      const p = this.doc.getPage(i);
      p.drawText(`Page ${i + 1} of ${total}`, {
        x: this.MARGIN, y: this.FOOTER_Y,
        size: 8, font: this.font, color: COL.muted,
      });
    }

    const bytes = await this.doc.save();
    return new NextResponse(Buffer.from(bytes), {
      status: 200,
      headers: {
        "Content-Type": "application/pdf",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  }
}

// ─── Executive Summary & Recommendations builder functions ──────────────

function buildExecutiveSummary(
  patterns: Record<string, unknown>[],
  anomalies: Record<string, unknown>[],
  methodsList: { method: string; count: number }[],
  totalRows: number,
  errorCount: number,
): string[] {
  const items: string[] = [];

  if (patterns.length > 0) {
    const top = patterns[0];
    items.push(`Primary Issue: ${findingTitle(top.pattern_key as string || "")}`);
  }

  if (patterns.length > 1) {
    const second = patterns[1];
    const pct = totalRows > 0 ? ((second.count as number) / totalRows * 100).toFixed(1) : "0";
    items.push(`Secondary Issue: ${findingTitle(second.pattern_key as string || "")} (${pct}% of rows)`);
  }

  if (methodsList.length > 0) {
    const topMethod = methodsList[0];
    items.push(`Most Affected Method: ${topMethod.method} (${topMethod.count.toLocaleString()} calls)`);
  }

  if (anomalies.length > 0) {
    const topAnom = anomalies[0];
    const date = (topAnom.detected_at as string || "").substring(0, 10);
    items.push(`Largest Error Spike: ${date} — ${(topAnom.error_count as number).toLocaleString()} errors (${(topAnom.deviation as number).toFixed(1)}× baseline)`);
  }

  const nAnomalies = anomalies.length;
  const primary = patterns.length > 0
    ? findingTitle(patterns[0].pattern_key as string || "")
    : "identified patterns";
  const action = nAnomalies > 0
    ? `Recommended Action: Investigate "${primary}" and review ${nAnomalies} anomalous spike period${nAnomalies > 1 ? "s" : ""}`
    : `Recommended Action: Investigate and address "${primary}"`;
  items.push(action);

  return items;
}

function buildRecommendations(
  patterns: Record<string, unknown>[],
  anomalies: Record<string, unknown>[],
  methodsList: { method: string; count: number }[],
  totalRows: number,
  errorCount: number,
): string[] {
  const recs: string[] = [];
  const rate = totalRows > 0 ? ((errorCount / totalRows) * 100).toFixed(1) : "0";

  if (patterns.length > 0) {
    const top = patterns[0];
    const pct = totalRows > 0 ? (((top.count as number) / totalRows) * 100).toFixed(1) : "0";
    recs.push(`Primary Issue: "${findingTitle(top.pattern_key as string || "")}" — ${pct}% of entries, highest impact`);
  }

  if (patterns.length > 1) {
    const second = patterns[1];
    const pct = totalRows > 0 ? (((second.count as number) / totalRows) * 100).toFixed(1) : "0";
    recs.push(`Secondary Issue: "${findingTitle(second.pattern_key as string || "")}" — ${pct}% of entries`);
  } else {
    recs.push("Secondary Issue: None identified");
  }

  if (methodsList.length > 0) {
    const topMethod = methodsList[0];
    recs.push(`Most Affected Method: ${topMethod.method} — ${topMethod.count.toLocaleString()} total calls, highest volume`);
  }

  if (anomalies.length > 0) {
    const topA = anomalies[0];
    const date = (topA.detected_at as string || "").substring(0, 10);
    recs.push(`Largest Anomaly: ${date} — ${(topA.error_count as number).toLocaleString()} errors (${(topA.deviation as number).toFixed(1)}× baseline of ${Math.round(topA.expected_count as number)})`);
  } else {
    recs.push("Largest Anomaly: No anomalies detected");
  }

  const target = patterns.length > 0
    ? findingTitle(patterns[0].pattern_key as string || "")
    : "error patterns";
  const anomalyAdvice = anomalies.length > 0
    ? ` and investigate ${anomalies.length} anomalous day${anomalies.length > 1 ? "s" : ""} with elevated error rates`
    : "";
  recs.push(`Suggested Investigation: Prioritize "${target}"${anomalyAdvice}. Error rate is ${rate}% (${errorCount.toLocaleString()}/${totalRows.toLocaleString()} rows) — ${parseFloat(rate) > 10 ? "above" : "within"} typical threshold`);

  return recs;
}

// ─── Route Handler ──────────────────────────────────────────────────────

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const numId = Number(id);

  const { data: analysis } = await serviceClient
    .from("log_analyses")
    .select("*")
    .eq("id", numId)
    .single();

  if (!analysis) {
    return NextResponse.json({ error: "Analysis not found" }, { status: 404 });
  }

  const { data: patternsData } = await serviceClient
    .from("log_patterns")
    .select("*")
    .eq("analysis_id", numId)
    .order("count", { ascending: false });
  const { data: anomaliesData } = await serviceClient
    .from("log_anomalies")
    .select("*")
    .eq("analysis_id", numId)
    .order("deviation", { ascending: false });
  const patterns = (patternsData ?? []) as Record<string, unknown>[];
  const anomalies = (anomaliesData ?? []) as Record<string, unknown>[];

  let methodsList: { method: string; count: number }[] = [];
  try {
    methodsList = JSON.parse((analysis.methods as string) || "[]");
  } catch {}
  const totalMethods = methodsList.reduce((s, m) => s + m.count, 0);

  // ── computed values ──
  const totalRows = analysis.total_rows as number;
  const errorCount = analysis.error_count as number;
  const errorRate = totalRows > 0 ? ((errorCount / totalRows) * 100).toFixed(1) : "0.0";
  const rangeStart = (analysis.time_range_start as string || "").substring(0, 10);
  const rangeEnd = (analysis.time_range_end as string || "").substring(0, 10);
  const cleanedFilename = (analysis.filename as string || "report.csv").replace(/^"(.*)"$/, "$1");
  const source = analysis.source as string || "Unknown";

  // ── Build Report ──

  const report = new PdfReport();
  await report.init();
  const { normal: font, bold: boldF } = report.getFonts();

  // ════════════════════════════════════════════════════
  // PAGE 1 — Executive Brief
  // ════════════════════════════════════════════════════

  // ── Report Header ──
  report.drawHeader(
    analysis.filename as string,
    source,
    analysis.time_range_start as string,
    analysis.time_range_end as string,
  );

  // ── File Information ──
  report.sectionTitle("File Information");
  report.textLine(`Source System:  ${source}`, 10, COL.secondary, font);
  report.textLine(`File:           ${cleanedFilename}`, 10, COL.secondary, font);
  report.textLine(`Period:         ${rangeStart} to ${rangeEnd}`, 10, COL.secondary, font);
  report.textLine(`Analysis ID:    ${analysis.id}`, 10, COL.secondary, font);
  report.textLine(`Generated:      ${new Date().toISOString().replace("T", " ").substring(0, 19)} UTC`, 10, COL.secondary, font);
  report.textLine(`Total Rows:     ${totalRows.toLocaleString()}`, 10, COL.secondary, font);
  report.addSpace(2);

  // ── Executive Summary ──
  report.sectionTitle("Executive Summary");
  const execLines = buildExecutiveSummary(patterns, anomalies, methodsList, totalRows, errorCount);
  if (execLines.length > 0) {
    report.drawBulletLines(execLines);
  } else {
    report.drawEmpty("Insufficient data for summary.");
  }

  // ── Key Metrics ──
  report.sectionTitle("Key Metrics");
  const days = (rangeStart && rangeEnd)
    ? Math.max(1, Math.round((Date.parse(rangeEnd + "T00:00:00") - Date.parse(rangeStart + "T00:00:00")) / 86400000))
    : 1;
  const avgErrorsPerDay = days > 0 ? (errorCount / days).toFixed(1) : "0";
  report.drawMetricGrid([
    ["Total Rows",       totalRows.toLocaleString()],
    ["Error Count",      errorCount.toLocaleString()],
    ["Error Rate",       `${errorRate}%`],
    ["Unique Patterns",  String(analysis.unique_errors as number)],
    ["Anomaly Spikes",   String(anomalies.length)],
    ["Avg Errors/Day",   avgErrorsPerDay],
  ]);

  // ── Top Findings ──
  report.sectionTitle("Top Findings");

  if (patterns.length === 0) {
    report.drawEmpty("No error patterns detected.");
  } else {
    const topN = Math.min(5, patterns.length);
    const cw = report["CONTENT_W"];
    const R = report["MARGIN"];
    const cols = [
      { x: R,                                           label: "#",    size: 8 },
      { x: R + Math.round(cw * 5 / 100),                label: "Severity", size: 8 },
      { x: R + Math.round(cw * 17 / 100),               label: "Finding", size: 8 },
      { x: R + Math.round(cw * 70 / 100),               label: "Count", size: 8, align: "right" as const },
      { x: R + Math.round(cw * 85 / 100),               label: "First Seen", size: 8, align: "right" as const },
    ];

    report.drawTable(cols, patterns.slice(0, topN), (p, i) => {
      const cnt = p.count as number;
      const freqSev = frequencySeverity(cnt);
      const pk = findingTitle(p.pattern_key as string || "");
      const first = (p.first_seen as string || "").substring(0, 10);
      return [
        { x: cols[0].x, text: String(i + 1), size: 8, color: COL.secondary },
        { x: cols[1].x, text: freqSev.label, size: 7, color: freqSev.color, font: boldF },
        { x: cols[2].x, text: pk, size: 7, color: COL.primary },
        { x: cols[3].x, text: String(cnt), size: 8, color: COL.primary, font: boldF, align: "right" },
        { x: cols[4].x, text: first, size: 7, color: COL.muted, align: "right" },
      ];
    });
  }

  // ── Recommendations ──
  report.sectionTitle("Recommendations");
  const recs = buildRecommendations(patterns, anomalies, methodsList, totalRows, errorCount);
  report.drawBulletLines(recs);

  // ════════════════════════════════════════════════════
  // PAGE 2 — Patterns & Anomalies
  // ════════════════════════════════════════════════════

  report.ensureSpace(700);
  report.drawRunningHeader();

  // ── Top Findings ──
  report.sectionTitle("3.  Top Findings");

  if (patterns.length === 0) {
    report.drawEmpty("No error patterns detected.");
  } else {
    const cw = report["CONTENT_W"];
    const R = report["MARGIN"];
    const patCols = [
      { x: R,                                           label: "#",    size: 8 },
      { x: R + Math.round(cw * 5 / 100),                label: "Severity", size: 8 },
      { x: R + Math.round(cw * 17 / 100),               label: "Finding", size: 8 },
      { x: R + Math.round(cw * 70 / 100),               label: "Count", size: 8, align: "right" as const },
      { x: R + Math.round(cw * 85 / 100),               label: "First Seen", size: 8, align: "right" as const },
    ];

    report.drawTable(patCols, patterns, (p, i) => {
      const cnt = p.count as number;
      const freqSev = frequencySeverity(cnt);
      const pk = findingTitle(p.pattern_key as string || "");
      const first = (p.first_seen as string || "").substring(0, 10);
      return [
        { x: patCols[0].x, text: String(i + 1), size: 8, color: COL.secondary },
        { x: patCols[1].x, text: freqSev.label, size: 7, color: freqSev.color, font: boldF },
        { x: patCols[2].x, text: pk, size: 7, color: COL.primary },
        { x: patCols[3].x, text: String(cnt), size: 8, color: COL.primary, font: boldF, align: "right" },
        { x: patCols[4].x, text: first, size: 7, color: COL.muted, align: "right" },
      ];
    });
  }

  // ── Anomaly Spikes ──
  report.sectionTitle("4.  Anomaly Spikes");

  if (anomalies.length === 0) {
    report.drawEmpty("No anomaly spikes detected.");
  } else {
    const anomCols = [
      { x: report["MARGIN"],                          label: "Date",    size: 8 },
      { x: report["MARGIN"] + 75,                     label: "Description", size: 8 },
      { x: report["MARGIN"] + 250,                    label: "Errors", size: 8 },
      { x: report["PAGE_W"] - report["MARGIN"] - 130, label: "Baseline", size: 8, align: "right" as const },
      { x: report["PAGE_W"] - report["MARGIN"] - 65,  label: "× Fold", size: 8, align: "right" as const },
    ];

    report.drawTable(anomCols, anomalies, (a) => {
      const sev = (a.severity as string) || "medium";
      const sevColor = sev === "high" ? COL.red : COL.amber;
      const day = (a.detected_at as string || "").substring(0, 10);
      const desc = anomalySummary(a.description as string || "");
      return [
        { x: anomCols[0].x, text: day,  size: 8, color: sevColor,  font: boldF },
        { x: anomCols[1].x, text: desc, size: 7, color: COL.primary },
        { x: anomCols[2].x, text: String(a.error_count as number), size: 8, color: COL.primary },
        { x: anomCols[3].x, text: String(Math.round(a.expected_count as number)), size: 8, color: COL.muted, align: "right" },
        { x: anomCols[4].x, text: String((a.deviation as number).toFixed(1)) + "×", size: 8, color: sevColor, align: "right" },
      ];
    });
  }

  // ════════════════════════════════════════════════════
  // PAGE 3 — Methods & Supporting Statistics
  // ════════════════════════════════════════════════════

  report.ensureSpace(700);
  report.drawRunningHeader();

  // ── Methods ──
  report.sectionTitle("5.  Methods");

  if (methodsList.length === 0) {
    report.drawEmpty("No methods recorded.");
  } else {
    const mCols = [
      { x: report["MARGIN"],                          label: "Method", size: 8 },
      { x: report["PAGE_W"] - report["MARGIN"] - 80,  label: "Count", size: 8, align: "right" as const },
    ];

    report.drawTable(mCols, methodsList, (m) => [
      { x: mCols[0].x, text: m.method, size: 7, color: COL.primary },
      { x: mCols[1].x, text: String(m.count), size: 8, color: COL.primary, font: boldF, align: "right" },
    ]);
  }

  // ── Supporting Statistics ──
  report.sectionTitle("6.  Supporting Statistics");

  const statsData: [string, string][] = [
    ["Total Rows", totalRows.toLocaleString()],
    ["Error Count", errorCount.toLocaleString()],
    ["Error Rate", `${errorRate}%`],
    ["Unique Error Patterns", String(analysis.unique_errors as number)],
    ["Anomalous Days", String(anomalies.length)],
    ["Distinct Methods", String(methodsList.length)],
    ["Total Method Calls", totalMethods.toLocaleString()],
    ["Date Range", `${rangeStart} to ${rangeEnd}`],
    ["Analysis ID", String(analysis.id)],
    ["Source File", cleanedFilename],
    ["Source System", source],
  ];

  const sCols = [
    { x: report["MARGIN"],                          label: "Metric", size: 8 },
    { x: report["PAGE_W"] - report["MARGIN"] - 80,  label: "Value", size: 8, align: "right" as const },
  ];

  report.drawTable(sCols, statsData, (row) => [
    { x: sCols[0].x, text: row[0], size: 8, color: COL.secondary, font },
    { x: sCols[1].x, text: row[1], size: 8, color: COL.primary, font: boldF, align: "right" },
  ]);

  // ── End of Report ──
  report.drawEndMarker();

  // ── Finalise ──
  const outName = `analysis-${analysis.id}-${sanitize(cleanedFilename).replace(/\.csv$/i, "")}.pdf`;
  return report.finish(outName);
}
