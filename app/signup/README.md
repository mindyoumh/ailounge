# `/signup` — Create Account Page

## Route

`/signup` — Public (no auth required).

## Page

Client component (`"use client"`) with a card centered on screen:

| Feature | Detail |
|---------|--------|
| Email/password form | `supabase.auth.signUp()` with `emailRedirectTo` |
| Domain gate | Client-side check: only `@mindyou.com.ph` emails allowed, shows error inline |
| Validation | Password min 6 characters (HTML `minLength={6}`) |
| Redirect | Pushes to `/feed` on success |
| Error display | Red inline error message on auth failure or domain rejection |
| Loading state | Button shows "Creating account..." and disables |
| Link to login | `<Link href="/login">` at bottom |

### Server-side domain gate (Auth Hook)

A Supabase Auth Hook (`supabase/functions/before-signup/index.ts`) rejects signups from non-`@mindyou.com.ph` emails **before** the user is created — belts-and-suspenders with the client-side check. The `handle_new_user()` trigger then assigns the correct role (`intern` for `%intern%@...`, otherwise `dev`).

### Dependencies

- `getBrowserSupabase()` from `@/src/db/browser-client`
- Supabase Auth (email/password only — no OAuth on signup)
- Logo from `@/src/image/MindYou_LogoBlue.png`

### Protection

Login and signup pages are excluded from the auth middleware (see `proxy.ts` — listed in `PUBLIC_ROUTES`).
