Deno.serve(async (req) => {
  const { user } = await req.json();
  const email = user?.email ?? "";

  if (!email.endsWith("@mindyou.com.ph")) {
    return Response.json(
      { error: { message: "Only @mindyou.com.ph emails allowed" } },
      { status: 200 },
    );
  }

  return Response.json({ result: { user } }, { status: 200 });
});
