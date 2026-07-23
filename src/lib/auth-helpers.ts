import { NextRequest, NextResponse } from "next/server";
import { createServerClient } from "@supabase/ssr";
import { serviceClient } from "@/src/db/service-client";

export async function requireRole(
  request: NextRequest,
  roles: string[],
): Promise<NextResponse | null> {
  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll: () => request.cookies.getAll(),
        setAll: () => {},
      },
    },
  );

  const { data: { user } } = await supabase.auth.getUser();
  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { data: roleData } = await serviceClient
    .from("user_roles")
    .select("role")
    .eq("user_id", user.id)
    .maybeSingle();

  if (!roleData || !roles.includes(roleData.role)) {
    return NextResponse.json(
      { error: `Forbidden — requires one of roles: ${roles.join(", ")}` },
      { status: 403 },
    );
  }

  return null;
}
