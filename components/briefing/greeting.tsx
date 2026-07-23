"use client";

import { useUser } from "@/components/auth-provider";
import { Sparkles } from "lucide-react";

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return "Good morning";
  if (hour < 17) return "Good afternoon";
  return "Good evening";
}

function getName(email: string): string {
  return email.split("@")[0]?.replace(/[._]/g, " ") ?? "there";
}

const WEEKDAYS = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];
const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

function formatDate(date: Date): string {
  return `${WEEKDAYS[date.getDay()]}, ${MONTHS[date.getMonth()]} ${date.getDate()}`;
}

export function Greeting({ totalItems }: { totalItems: number }) {
  const { user } = useUser();
  const email = user?.email ?? "";
  const name = getName(email);
  const greeting = getGreeting();
  const today = formatDate(new Date());

  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <h1 className="text-4xl font-bold text-foreground font-display tracking-tight">
          {greeting}, <span className="text-accent-vibrant">{name}</span>
        </h1>
        <p className="text-md text-muted-foreground mt-0.5">
          {today} &middot; {totalItems.toLocaleString()} items
        </p>
      </div>
      <div className="hidden sm:flex items-center gap-1.5 text-xs text-muted-foreground">
        {" "}
      </div>
    </div>
  );
}
