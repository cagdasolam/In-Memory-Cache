const { spawn } = require("child_process");
const http = require("http");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function request(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const postData = body ? JSON.stringify(body) : null;
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port: 3000,
        path,
        method,
        headers: {
          "Content-Type": "application/json",
          ...(postData
            ? { "Content-Length": Buffer.byteLength(postData) }
            : {}),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(data) });
          } catch {
            resolve({ statusCode: res.statusCode, body: data });
          }
        });
      },
    );
    req.on("error", reject);
    if (postData) req.write(postData);
    req.end();
  });
}

async function main() {
  console.log("=== [1/5] Starting Rust In-Memory-Cache Server ===");
  const rustProc = spawn("wsl", [
    "-d",
    "Ubuntu",
    "bash",
    "-lc",
    "cd /mnt/c/cagdas/projects/In-Memory-Cache && ./target/release/in-memory-cache",
  ]);

  rustProc.stdout.on("data", (d) =>
    console.log(`[Rust Server] ${d.toString().trim()}`),
  );
  rustProc.stderr.on("data", (d) =>
    console.error(`[Rust Server Err] ${d.toString().trim()}`),
  );

  // Wait for Rust server to be ready
  await sleep(1500);

  console.log("=== [2/5] Starting NestJS Application ===");
  const nestProc = spawn("node", ["dist/main.js"], {
    cwd: __dirname,
    env: {
      ...process.env,
      PORT: "3000",
      CACHE_HOST: "127.0.0.1",
      CACHE_PORT: "6379",
    },
  });

  nestProc.stdout.on("data", (d) =>
    console.log(`[NestJS] ${d.toString().trim()}`),
  );
  nestProc.stderr.on("data", (d) =>
    console.error(`[NestJS Err] ${d.toString().trim()}`),
  );

  // Wait for NestJS to initialize
  await sleep(3000);

  try {
    console.log(
      "\n=== [3/5] Executing E2E REST Requests against NestJS + Rust Cache ===\n",
    );

    // 1. Strings & TTL
    console.log("-> Testing Strings & TTL (POST & GET /demo/kv)...");
    const setKvRes = await request("POST", "/demo/kv", {
      key: "session:user_token_99",
      value: { userId: 42, role: "admin" },
      ttl: 60,
    });
    console.log("   POST /demo/kv Response:", setKvRes.body);
    if (!setKvRes.body.success) throw new Error("SET KV failed");

    const getKvRes = await request("GET", "/demo/kv/session:user_token_99");
    console.log("   GET /demo/kv Response:", getKvRes.body);
    if (
      getKvRes.body.value.userId !== 42 ||
      getKvRes.body.ttlRemainingSeconds <= 0
    ) {
      throw new Error("GET KV verification failed");
    }

    // 2. Hashes
    console.log("\n-> Testing Hashes (POST & GET /demo/users)...");
    const userPayload = {
      id: "usr_777",
      name: "Çağdaş",
      email: "cagdas@example.com",
      role: "Staff Engineer",
      updatedAt: "2026-09-12",
    };
    const postUserRes = await request("POST", "/demo/users", userPayload);
    console.log("   POST /demo/users Response:", postUserRes.body);

    const getUserRes = await request("GET", "/demo/users/usr_777");
    console.log("   GET /demo/users/usr_777 Response:", getUserRes.body);
    if (
      getUserRes.body.name !== "Çağdaş" ||
      getUserRes.body.role !== "Staff Engineer"
    ) {
      throw new Error("Hash user profile verification failed");
    }

    // 3. Lists
    console.log("\n-> Testing Lists (POST & GET /demo/tasks)...");
    await request("POST", "/demo/tasks", {
      task: "Task #1 - Compile Cache Engine",
    });
    await request("POST", "/demo/tasks", { task: "Task #2 - Run Benchmarks" });
    const getTasksRes = await request("GET", "/demo/tasks");
    console.log("   GET /demo/tasks Response:", getTasksRes.body);
    if (getTasksRes.body.count < 2)
      throw new Error("List tasks verification failed");

    // 4. Sets
    console.log("\n-> Testing Sets (POST & GET /demo/tags)...");
    await request("POST", "/demo/tags", {
      tags: ["rust", "tokio", "jemalloc", "nestjs", "rust"],
    });
    const getTagsRes = await request("GET", "/demo/tags");
    console.log("   GET /demo/tags Response:", getTagsRes.body);
    if (
      !getTagsRes.body.tags.includes("rust") ||
      !getTagsRes.body.tags.includes("nestjs")
    ) {
      throw new Error("Set tags verification failed");
    }

    // 5. Pub/Sub
    console.log("\n-> Testing Pub/Sub (POST /demo/publish)...");
    const pubRes = await request("POST", "/demo/publish", {
      message: "Greetings from NestJS Client E2E Test!",
    });
    console.log("   POST /demo/publish Response:", pubRes.body);
    if (!pubRes.body.success) throw new Error("Pub/Sub verification failed");

    await sleep(500); // Give time for subscriber log output

    console.log(
      "\n✅ ALL NESTJS + RUST IN-MEMORY CACHE TESTS PASSED SUCCESSFULLY! ✅\n",
    );
  } finally {
    console.log("=== [4/5] Terminating NestJS Application ===");
    nestProc.kill("SIGINT");

    console.log("=== [5/5] Terminating Rust Server Cleanly ===");
    rustProc.kill("SIGINT");

    // Extra cleanup just in case
    setTimeout(() => {
      process.exit(0);
    }, 1500);
  }
}

main().catch((err) => {
  console.error("Test failed with error:", err);
  process.exit(1);
});
