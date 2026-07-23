# `/login` — Sign In Page

## Route

`/login` — Public (no auth required).

## Page

Client component (`"use client"`) with a glassmorphism card centered on screen:

### LoginForm (inner component)

| Feature | Detail |
|---------|--------|
| Email/password form | `supabase.auth.signInWithPassword()` |
| GitHub OAuth | `supabase.auth.signInWithOAuth({ provider: "github" })` |
| Redirect support | Reads `?redirect=` from URL params, defaults to `/feed` |
| Error display | Red inline error message on auth failure |
| Loading state | Button shows "Signing in..." and disables during request |
| Link to signup | `<Link href="/signup">` at bottom |

### Dependencies

- `getBrowserSupabase()` from `@/src/db/browser-client`
- Supabase Auth (email/password + GitHub OAuth)
- Logo from `@/src/image/MindYou_LogoBlue.png`

### Protection

Login and signup pages are excluded from the auth middleware (see `proxy.ts` — listed in `PUBLIC_ROUTES`).
