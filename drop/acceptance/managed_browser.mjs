// A real Chromium session for each owner. The fixture mailbox is ephemeral.
import { createRequire } from "node:module";
import { readdir, readFile, unlink } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
const require = createRequire(
  new URL("../../ui/package.json", import.meta.url),
);
const { chromium } = require("playwright");
const [mail, ...hosts] = process.argv.slice(2);
const owners = ["owner@crew-drop.invalid", "second@crew-drop.invalid"];
const browser = await chromium.launch({
  args: [
    "--no-proxy-server",
    "--host-resolver-rules=MAP *.apps.example.test 127.0.0.1",
  ],
});
try {
  await Promise.all(
    hosts.map(async (host, i) => {
      const page = await browser.newPage({ ignoreHTTPSErrors: true });
      await page.goto(`https://${host}/`);
      await page.getByLabel("Email", { exact: true }).fill(owners[i]);
      await page
        .getByRole("button", { name: "Email me a sign-in code" })
        .click();
      const field = page.getByLabel("Code from the email");
      await field.waitFor();
      if (
        !(await page.getByRole("status").innerText()).includes(
          "within 15 minutes",
        )
      ) {
        throw Error("Expected code-only login instructions");
      }
      const deadline = Date.now() + 18000;
      let name;
      while (Date.now() < deadline && !name) {
        name = (await readdir(mail)).find((entry) =>
          entry.startsWith(`code-${owners[i]}-`),
        );
        if (!name) await sleep(150);
      }
      if (!name) throw Error("Managed email did not reach its bound owner");
      const file = mail + "/" + name;
      const code = (await readFile(file, "utf8")).trim();
      await unlink(file);
      const success = page.waitForResponse(
        (res) => res.url().endsWith("/auth-with-otp-code") && res.ok(),
      );
      await field.fill(code);
      const auth = await (await success).json();
      if (!auth.token || auth.record.email !== owners[i])
        throw Error("Wrong authenticated Crew session");
      await page.waitForFunction(
        () => !document.querySelector("input[name=code]"),
      );
      await page.close();
    }),
  );
} finally {
  await browser.close();
}
