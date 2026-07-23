import Link from "next/link";
import { MessageSquare, ArrowRight } from "lucide-react";

interface PromptItem {
  id: number;
  title: string;
  content: string;
  category: string;
  description: string | null;
  usage_count: number;
}

const CATEGORY_LABELS: Record<string, string> = {
  code_review: "Code Review",
  debugging: "Debugging",
  architecture: "Architecture",
  incident_analysis: "Incident Analysis",
  refactoring: "Refactoring",
  security_audit: "Security Audit",
  documentation: "Documentation",
  intern_mentoring: "Intern Mentoring",
  stakeholder_emails: "Stakeholder Emails",
};

export function FeaturedPrompt({ item }: { item: PromptItem | null }) {
  if (!item) return null;

  return (
    <div className="rounded-xl border border-border bg-card p-4 transition-all duration-200 hover:shadow-sm">
      <div className="flex items-center gap-2 mb-2.5">
        <div className="flex h-6 w-6 items-center justify-center rounded-md bg-accent/70">
          <MessageSquare className="h-3.5 w-3.5 text-muted-foreground" />
        </div>
        <h3 className="text-sm font-semibold text-foreground font-display">Featured Prompt</h3>
      </div>
      <Link href="/prompts" className="group block">
        <p className="text-sm font-medium text-foreground/90 group-hover:text-accent-vibrant transition-colors truncate">
          {item.title}
        </p>
        {item.description && (
          <p className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
            {item.description}
          </p>
        )}
        <div className="flex items-center justify-between mt-2">
          <span className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[9px] font-medium font-mono bg-accent/80 text-muted-foreground">
            {CATEGORY_LABELS[item.category] || item.category}
          </span>
          <ArrowRight className="h-3.5 w-3.5 text-muted-foreground/50 group-hover:text-accent-vibrant transition-colors" />
        </div>
      </Link>
    </div>
  );
}
