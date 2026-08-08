/** Poll until `check` passes. Avoids fixed sleeps, which are slow and flaky. */
export async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(1);
  }
  throw new Error(`condition not met within ${timeoutMs}ms`);
}
