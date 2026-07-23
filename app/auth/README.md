# `/auth` — Authentication API Routes

## Routes

### `GET /auth/callback`

OAuth callback handler for GitHub login flow.

| Detail | Value |
|--------|-------|
| File | `callback/route.ts` |
| Type | Route Handler (Edge-compatible) |
| Public | Yes (excluded from auth middleware) |

**Flow:**

1. GitHub redirects user to `{origin}/auth/callback?code=...`
2. Handler calls `supabase.auth.exchangeCodeForSession(code)`
3. On success → redirects to `/feed` (or `?next=` param)
4. On failure → redirects to `/login?error=auth_failed`

**Implementation:**

Uses `@supabase/ssr` `createServerClient` directly (not `getServerSupabase()`) because this is a standalone route handler that receives raw `Request` cookies rather than Next.js `cookies()`.

### Dependencies

- `@supabase/ssr` — `createServerClient`
- `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`
