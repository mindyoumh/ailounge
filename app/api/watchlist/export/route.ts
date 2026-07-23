import { NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { PDFDocument, StandardFonts, rgb, PDFFont } from "pdf-lib";

export const dynamic = "force-dynamic";

function parseSemver(v: string | null): { major: number; minor: number; patch: number } | null {
  if (!v) return null;
  const parts = v.replace(/^[vV]/, "").split(".");
  const [major, minor, patch] = parts.map(Number);
  if (isNaN(major)) return null;
  return { major, minor: minor ?? 0, patch: patch ?? 0 };
}

function getVersionLabel(installed: string | null, latest: string | null): string {
  if (!installed || !latest) return "—";
  const a = parseSemver(installed);
  const b = parseSemver(latest);
  if (!a || !b) return "—";
  if (a.major === b.major && a.minor === b.minor && a.patch === b.patch) return "Up to date";
  if (a.major !== b.major) return "Major update";
  if (a.minor !== b.minor) return `${b.minor - a.minor} minor behind`;
  return `${b.patch - a.patch} patch${b.patch - a.patch > 1 ? "es" : ""} behind`;
}

const WIN_ANSI_HIGH = new Set([
  0x20AC, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
  0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x017D, 0x2018,
  0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC,
  0x2122, 0x0161, 0x203A, 0x0153, 0x017E, 0x0178,
]);

function sanitize(text: string): string {
  text = text
    .replace(/→/g, "->")
    .replace(/—/g, "-")
    .replace(/–/g, "-")
    .replace(/•/g, "*")
    .replace(/['']/g, "'")
    .replace(/[""]/g, '"')
    .replace(/…/g, "...");
  const out: string[] = [];
  for (const c of text) {
    const cp = c.codePointAt(0)!;
    if (cp < 0x80 || (cp >= 0xA0 && cp <= 0xFF) || WIN_ANSI_HIGH.has(cp)) {
      out.push(c);
    } else {
      out.push("?");
    }
  }
  return out.join("");
}

function rgbC(r: number, g: number, b: number) {
  return rgb(r, g, b);
}

const COL = {
  primary: rgbC(0.15, 0.15, 0.15),
  secondary: rgbC(0.35, 0.35, 0.35),
  muted: rgbC(0.55, 0.55, 0.55),
  border: rgbC(0.82, 0.82, 0.82),
  headerBg: rgbC(0.93, 0.93, 0.93),
  white: rgbC(1, 1, 1),
  green: rgbC(0.15, 0.6, 0.35),
  amber: rgbC(0.8, 0.55, 0.1),
  red: rgbC(0.8, 0.2, 0.2),
  accent: rgbC(0.25, 0.4, 0.85),
};

export async function GET() {
  const { data: items } = await serviceClient
    .from("watchlist_items")
    .select("*")
    .order("category")
    .order("name");

  if (!items || items.length === 0) {
    return NextResponse.json({ error: "Watchlist is empty" }, { status: 404 });
  }

  const doc = await PDFDocument.create();
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const bold = await doc.embedFont(StandardFonts.HelveticaBold);

  const MARGIN = 50;
  const PAGE_W = 612;
  const PAGE_H = 792;
  const CONTENT_W = PAGE_W - 2 * MARGIN;

  let page = doc.addPage([PAGE_W, PAGE_H]);
  let y = PAGE_H - MARGIN;
  let pageNum = 1;

  function drawText(text: string, x: number, baseline: number, size: number, color: ReturnType<typeof rgb>, f: PDFFont) {
    page.drawText(sanitize(text), { x, y: baseline, size, font: f, color });
  }

  function drawLine(yPos: number, thickness = 0.5, color?: ReturnType<typeof rgb>) {
    page.drawLine({
      start: { x: MARGIN, y: yPos },
      end: { x: PAGE_W - MARGIN, y: yPos },
      thickness,
      color: color ?? COL.border,
    });
  }

  function ensureSpace(needed: number) {
    if (y - needed < 50) {
      drawFooter();
      page = doc.addPage([PAGE_W, PAGE_H]);
      y = PAGE_H - MARGIN;
      pageNum++;
      drawRunningHeader();
    }
  }

  function drawRunningHeader() {
    drawText("Stack Watchlist Report (continued)", MARGIN, y - 10, 10, COL.muted, font);
    y -= 16;
  }

  function drawFooter() {
    const total = doc.getPageCount();
    for (let i = 0; i < total; i++) {
      const p = doc.getPage(i);
      p.drawText(sanitize(`Page ${i + 1} of ${total}`), {
        x: MARGIN, y: 40, size: 8, font, color: COL.muted,
      });
    }
  }

  // ── Header ──
  drawText("Stack Watchlist Report", MARGIN, y - 24, 22, COL.primary, bold);
  y -= 30;

  const dateStr = new Date().toLocaleDateString("en-US", {
    year: "numeric", month: "long", day: "numeric",
  });
  drawText(`Generated: ${dateStr}`, MARGIN, y - 10, 10, COL.secondary, font);
  y -= 14;
  drawText(`Total items: ${items.length}`, MARGIN, y - 10, 10, COL.secondary, font);
  y -= 20;
  drawLine(y, 1);
  y -= 14;

  // ── Table header ──
  const colWidths = [170, 80, 130, 70, 72];
  const colStarts: number[] = [];
  let cx = MARGIN;
  for (const w of colWidths) {
    colStarts.push(cx);
    cx += w;
  }

  function drawTableHeader() {
    ensureSpace(20);
    page.drawRectangle({
      x: MARGIN, y: y - 16, width: CONTENT_W, height: 16,
      color: COL.headerBg, borderColor: COL.border, borderWidth: 1,
    });
    const headers = ["Name", "Category", "Version", "Vulns", "Risk"];
    for (let i = 0; i < headers.length; i++) {
      drawText(headers[i], colStarts[i] + 4, y - 11, 8, COL.secondary, bold);
    }
    y -= 18;
  }

  function drawRow(name: string, category: string, version: string, vulns: string, riskLabel: string, riskColor: ReturnType<typeof rgb>) {
    const rowH = 18;
    ensureSpace(rowH + 4);

    const lines = [name, category, version, vulns, riskLabel];
    for (let i = 0; i < lines.length; i++) {
      drawText(lines[i], colStarts[i] + 4, y - 9, 8, i === 4 ? riskColor : COL.primary, i === 4 ? bold : font);
    }

    y -= rowH;
    drawLine(y + 2, 0.3);
  }

  drawTableHeader();

  for (const item of items) {
    const installed = (item.installed_version as string) || "—";
    const latest = (item.latest_version as string) || "—";
    const versionStr = `${installed} → ${latest}  ${getVersionLabel(item.installed_version as string, item.latest_version as string)}`;

    let vulnsStr = "—";
    let riskLabel = (item.risk_level as string) || "low";
    let riskColor = COL.green;
    if (riskLabel === "medium") riskColor = COL.amber;
    if (riskLabel === "high") riskColor = COL.red;

    try {
      const parsed = JSON.parse((item.known_vulns as string) || "{}");
      if (parsed.cves && parsed.cves.length > 0) {
        vulnsStr = `${parsed.totalCount} CVE${parsed.totalCount > 1 ? "s" : ""}`;
      } else {
        vulnsStr = "0";
      }
    } catch {}

    drawRow(
      (item.name as string) || "",
      (item.category as string) || "—",
      versionStr,
      vulnsStr,
      riskLabel.charAt(0).toUpperCase() + riskLabel.slice(1),
      riskColor,
    );
  }

  drawFooter();

  const bytes = await doc.save();
  return new NextResponse(Buffer.from(bytes), {
    status: 200,
    headers: {
      "Content-Type": "application/pdf",
      "Content-Disposition": `attachment; filename="stack-watchlist-${dateStr.replace(/\s+/g, "-")}.pdf"`,
    },
  });
}
