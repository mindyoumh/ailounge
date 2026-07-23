"use client";

import { createContext, useCallback, useContext, useEffect, useState } from "react";
import { getBrowserSupabase } from "@/src/db/browser-client";
import type { User } from "@supabase/supabase-js";

interface AuthContextValue {
  user: User | null;
  role: string | null;
  loading: boolean;
  signOut: () => Promise<void>;
  refreshRole: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue>({
  user: null,
  role: null,
  loading: true,
  signOut: async () => {},
  refreshRole: async () => {},
});

async function fetchRole(userId: string): Promise<string> {
  try {
    const supabase = getBrowserSupabase();
    const { data, error } = await supabase
      .from("user_roles")
      .select("role")
      .eq("user_id", userId)
      .maybeSingle();

    if (error) {
      console.error("[auth-provider] user_roles query error:", error);
      return "intern";
    }

    if (!data) {
      return "intern";
    }

    return data.role;
  } catch (err) {
    console.error("[auth-provider] fetchRole error:", err);
    return "intern";
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refreshRole = useCallback(async () => {
    const supabase = getBrowserSupabase();
    const { data: { user: u } } = await supabase.auth.getUser();
    if (!u) return;
    const r = await fetchRole(u.id);
    setRole(r);
  }, []);

  useEffect(() => {
    const supabase = getBrowserSupabase();

    supabase.auth
      .getUser()
      .then(async ({ data: { user: u } }) => {
        setUser(u);
        if (u) {
          const r = await fetchRole(u.id);
          setRole(r);
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, session) => {
      const u = session?.user ?? null;
      setUser(u);
      if (u) {
        fetchRole(u.id)
          .then((r) => setRole(r))
          .catch(() => {});
      } else {
        setRole(null);
      }
      setLoading(false);
    });

    return () => subscription.unsubscribe();
  }, []);

  const signOut = useCallback(async () => {
    await getBrowserSupabase().auth.signOut();
    setUser(null);
    setRole(null);
  }, []);

  return (
    <AuthContext.Provider value={{ user, role, loading, signOut, refreshRole }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useUser() {
  return useContext(AuthContext);
}
