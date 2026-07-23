"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { getBrowserSupabase } from "@/src/db/browser-client";
import logoImage from "@/src/image/MindYou_LogoBlue.png";
import Link from "next/link";

export default function SignupPage() {
  const router = useRouter();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSignup(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);

    if (!email.endsWith("@mindyou.com.ph")) {
      setError("Only @mindyou.com.ph emails allowed to sign up");
      setLoading(false);
      return;
    }

    const supabase = getBrowserSupabase();
    try {
      const { error } = await supabase.auth.signUp({
        email,
        password,
        options: { emailRedirectTo: `${location.origin}/auth/callback` },
      });

      if (error) {
        setError(error.message);
        setLoading(false);
        return;
      }

      router.push("/feed");
    } catch {
      setError("Signup failed. Check your connection and try again.");
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div className="w-full max-w-sm animate-scale-in">
        <div className="rounded-2xl border border-border/50 bg-card/50 backdrop-blur-xl p-8">
          <div className="flex flex-col items-center mb-8">
            <Image src={logoImage} alt="Mind You" className="h-12 w-auto mb-4" />
            <h1 className="text-xl font-bold text-foreground font-display tracking-tight">
              Create an account
            </h1>
            <p className="text-sm text-muted-foreground mt-1">
              Start your own dashboard
            </p>
            <p className="text-xs text-muted-foreground/60 mt-2">
              Only <span className="font-medium text-foreground/80">@mindyou.com.ph</span> emails allowed
            </p>
          </div>

          <form onSubmit={handleSignup} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-foreground mb-1.5">
                Email
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@example.com"
                required
                className="w-full rounded-lg border border-border bg-background px-3 py-2.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring transition-all"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-foreground mb-1.5">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="At least 6 characters"
                required
                minLength={6}
                className="w-full rounded-lg border border-border bg-background px-3 py-2.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring transition-all"
              />
            </div>

            {error && (
              <p className="text-xs text-red-500 bg-red-500/10 rounded-lg px-3 py-2">
                {error}
              </p>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full rounded-lg bg-foreground text-background px-4 py-2.5 text-sm font-medium hover:opacity-90 transition-opacity disabled:opacity-50"
            >
              {loading ? "Creating account..." : "Create account"}
            </button>
          </form>

          <p className="text-center text-sm text-muted-foreground mt-6">
            Already have an account?{" "}
            <Link
              href="/login"
              className="text-accent-vibrant hover:underline font-medium"
            >
              Sign in
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
